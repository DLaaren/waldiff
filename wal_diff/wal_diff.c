/*-------------------------------------------------------------------------
 * 
 * Идея пока такая 
 * 
 * Сохраняем файл с логами во временное хранилище
 * Скидываем на диск сервера
 * Обрабатываем копию этого файла -- компрессуем wal записи
 * Скидываем на архив-диск теперь уже wal diff
 *
 * postgres/contrib/basic_archive
 * postgres/include/archive
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>

#include "archive/archive_module.h"
#include "common/int.h"
#include "miscadmin.h"
#include "storage/copydir.h"
#include "storage/fd.h"
#include "utils/guc.h"
#include "utils/memutils.h"

PG_MODULE_MAGIC;

static char *wal_directory = NULL;
static char *wal_diff_directory = NULL;

static bool check_archive_directory(char **newval, void **extra, GucSource source);
static bool create_wal_diff(const char *file, const char *destination);
static bool compare_files(const char *file1, const char *file2);
static void generate_temp_file_name(char *temp, const char *file);
static bool is_file_archived(const char *file, const char *destination, const char *archive_directory);
static void wal_diff_startup(ArchiveModuleState *state);
static bool wal_diff_configured(ArchiveModuleState *state);
static bool wal_diff_archive(ArchiveModuleState *state, const char *file, const char *path);
static void wall_diff_shutdown(ArchiveModuleState *state);

static const ArchiveModuleCallbacks wal_diff_callbacks = {
    .startup_cb = wal_diff_startup,
	.check_configured_cb = wal_diff_configured,
	.archive_file_cb = wal_diff_archive,
	.shutdown_cb = wall_diff_shutdown
};

/*
 * _PG_init
 *
 * Defines the module's GUC.
 */
void
_PG_init(void)
{
    DefineCustomStringVariable("wal_diff.wal_directory",
							   gettext_noop("Archive WAL destination directory."),
							   NULL,
							   &wal_directory,
							   "wal_directory",
							   PGC_SIGHUP,
							   0,
							   check_archive_directory, NULL, NULL);
							   
	DefineCustomStringVariable("wal_diff.wal_diff_directory",
							   gettext_noop("Archive WAL-diff destination directory."),
							   NULL,
							   &wal_diff_directory,
							   "wal_diff_directory",
							   PGC_SIGHUP,
							   0,
							   check_archive_directory, NULL, NULL);

	MarkGUCPrefixReserved("wal_diff");
}

/*
 * _PG_archive_module_init
 *
 * Returns the module's archiving callbacks.
 */
const ArchiveModuleCallbacks *
_PG_archive_module_init(void)
{
	return &wal_diff_callbacks;
}

// add checking if there "still temp" wal-diffs

/*
 * wal_diff_startup
 *
 * Creates the module's memory context.
 */
void 
wal_diff_startup(ArchiveModuleState *state)
{
    return;
}

/*
 * check_archive_directory
 *
 * Checks that the provided archive directory exists.
 */
static bool 
check_archive_directory(char **newval, void **extra, GucSource source)
{
	struct stat st;

	if (*newval == NULL || *newval[0] == '\0')
	{
		GUC_check_errmsg("Archive directory name is blank");
		return false;
	}

	if (strlen(*newval) >= MAXPGPATH)
	{
		GUC_check_errmsg("Archive directory name is too long");
		return false;
	}	
	
	if (stat(*newval, &st) != 0 || !S_ISDIR(st.st_mode))
	{
		GUC_check_errdetail("Specified archive directory does not exist: %m");

		if (pg_mkdir_p(*newval, 0700) != 0)
		{
			GUC_check_errmsg("Could not allocate specified directory: %m");
			return false;
		}
	}

	return true;
}

/*
 * wal_diff_configured
 *
 * Checks that wal_directory and wal_diff_directory are not blank.
 */
static bool 
wal_diff_configured(ArchiveModuleState *state)
{
    return wal_diff_directory != NULL && wal_diff_directory[0] != '\0' 
			&& wal_directory != NULL && wal_directory[0] != '\0';
}

/*
 * TODO:
 * 
 * Add funcionality for a scenario when we are recovering after crash
 */

/*
 * wal_diff_archive
 *
 * Archives one WAL file and WAL-diff file.
 * 
 * file -- just name of the WAL file 
 * path -- the full path including the WAL file name
 */
static bool 
wal_diff_archive(ArchiveModuleState *state, const char *file, const char *path)
{
	char wal_diff_destination[MAXPGPATH];
	char wal_destination[MAXPGPATH];
	char temp[MAXPGPATH + 256]; // temp location for creating WAL-diff

	snprintf(wal_destination, MAXPGPATH, "%s/%s", wal_directory, file);
	snprintf(wal_diff_destination, MAXPGPATH, "%s/%s", wal_diff_directory, file);

	/*
	 * We check if the file has been alreafy archived.
	 * This scenario is possible if the server chrashed after archiving the file
	 * but before renaming its .ready to .done
	 */
	if (!is_file_archived(path, wal_destination, wal_directory))
	{
		copy_file(path, wal_destination);
		ereport(LOG,
				errmsg("archived wal-file \"%s\"", file));
	}

	if (!is_file_archived(path, wal_diff_destination, wal_diff_directory))
	{
		generate_temp_file_name(temp, wal_diff_destination);
		copy_file(wal_destination, temp);

		if (!create_wal_diff(temp, wal_diff_destination))
		{
			ereport(ERROR,
					errmsg("error while creating WAL-diff"));
			return false;
		}

		ereport(LOG,
				errmsg("created WAL-diff for file \"%s\"", file));
	}

	return true;
}

/*
 * is_file_archived
 *
 * Returns whether the file has already been archived.
 */
static bool 
is_file_archived(const char *file, const char *destination, const char *archive_directory) {
	struct stat st;
	
	if (stat(file, &st) == 0)
	{
		if (compare_files(file, destination))
		{
			ereport(WARNING,
					errmsg("file \"%s\" already exists with identical contents",
							destination));

			//make sure that this file is fsynced to the disk
			fsync_fname(destination, false);
			fsync_fname(archive_directory, true);
			return true;
		}
		return false;
	}
	else if (errno != ENOENT)
	{
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not stat file \"%s\": %m", destination)));
		return false;
	}

	return false;
}

static void
generate_temp_file_name(char *temp, const char *file) {
	const size_t temp_size = MAXPGPATH + 256;
	struct timeval tv;
	uint64 epoch;

	gettimeofday(&tv, NULL);
	if (pg_mul_u64_overflow((uint64) 1000, (uint64) tv.tv_sec, &epoch) ||
		pg_add_u64_overflow(epoch, (uint64) (tv.tv_usec / 1000), &epoch))
		ereport(ERROR, errmsg("could not generate temporary file name for archiving"));

	snprintf(temp, temp_size, "%s.%s.%d." UINT64_FORMAT,
			 file, "temp", MyProcPid, epoch);
	// ereport(LOG, errmsg("temp name is \"%s\"", temp));
}

/*
 * compare_files
 *
 * Returns whether the contents of the files are the same.
 */
static bool
compare_files(const char *file1, const char *file2) 
{
#define CMP_BUF_SIZE (4096)
	char buf1[CMP_BUF_SIZE];
	char buf2[CMP_BUF_SIZE];
	int fd1;
	int fd2;
	bool ret = true;

	fd1 = OpenTransientFile(file1, O_RDONLY | PG_BINARY);
	if (fd1 < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", file1)));

	fd2 = OpenTransientFile(file2, O_RDONLY | PG_BINARY);
	if (fd2 < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", file2)));

	for (;;)
	{
		int			nbytes = 0;
		int			buf1_len = 0;
		int			buf2_len = 0;

		while (buf1_len < CMP_BUF_SIZE)
		{
			nbytes = read(fd1, buf1 + buf1_len, CMP_BUF_SIZE - buf1_len);
			if (nbytes < 0)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not read file \"%s\": %m", file1)));
			else if (nbytes == 0)
				break;

			buf1_len += nbytes;
		}

		while (buf2_len < CMP_BUF_SIZE)
		{
			nbytes = read(fd2, buf2 + buf2_len, CMP_BUF_SIZE - buf2_len);
			if (nbytes < 0)
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not read file \"%s\": %m", file2)));
			else if (nbytes == 0)
				break;

			buf2_len += nbytes;
		}

		if (buf1_len != buf2_len || memcmp(buf1, buf2, buf1_len) != 0)
		{
			ret = false;
			break;
		}
		else if (buf1_len == 0)
			break;
	}

	if (CloseTransientFile(fd1) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not close file \"%s\": %m", file1)));

	if (CloseTransientFile(fd2) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not close file \"%s\": %m", file2)));

	return ret;
}

/*
 * create_wal_diff
 *
 * Creates one WAL-diff file.
 */
static bool 
create_wal_diff(const char *file, const char *destination)
{
	(void) durable_rename(file, destination, ERROR);
	return true;
}

/*
 * wall_diff_shutdown
 *
 * Frees our allocated state.
 */
static void 
wall_diff_shutdown(ArchiveModuleState *state)
{
	void *data = state->private_data;

	if (data == NULL)
		return;
	
	pfree(data);
	state->private_data = NULL;
}
