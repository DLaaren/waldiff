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
#include <unistd.h>

#include "archive/archive_module.h"
#include "storage/copydir.h"
#include "storage/fd.h"
#include "utils/guc.h"
#include "utils/memutils.h"

PG_MODULE_MAGIC;

static char *wal_directory = NULL;
static char *wal_diff_directory = NULL;

static bool check_wal_directory(char **newval, void **extra, GucSource source);
static bool create_wal_diff();
static void generate_temp_name(char *temp);
static bool is_file_archived(char *file);
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

void
_PG_init(void)
{
    DefineCustomStringVariable("wal_diff.wal_directory",
							   gettext_noop("Archive WAL destination directory."),
							   NULL,
							   &wal_directory,
							   "",
							   PGC_SIGHUP,
							   0,
							   check_wal_directory, NULL, NULL);
							   
	DefineCustomStringVariable("wal_diff.wal_diff_directory",
							   gettext_noop("Archive WAL-diff destination directory."),
							   NULL,
							   &wal_diff_directory,
							   "",
							   PGC_SIGHUP,
							   0,
							   check_wal_directory, NULL, NULL);

	MarkGUCPrefixReserved("wal_diff");
}

const ArchiveModuleCallbacks *
_PG_archive_module_init(void)
{
	return &wal_diff_callbacks;
}

void 
wal_diff_startup(ArchiveModuleState *state)
{
    ///
}

// typedef bool (*GucStringCheckHook) (char **newval, void **extra, GucSource source);
static bool 
check_wal_directory(char **newval, void **extra, GucSource source)
{

}

static bool 
wal_diff_configured(ArchiveModuleState *state)
{
    return wal_diff_directory != NULL && wal_diff_directory[0] != '\0' && wal_directory != NULL && wal_directory[0] != '\0';
}

// file -- just name of the WAL file 
// path -- the full path including the WAL file name
static bool 
wal_diff_archive(ArchiveModuleState *state, const char *file, const char *path)
{
	char wal_diff_destination[MAXPGPATH];
	char wal_destination[MAXPGPATH];
	char temp[MAXPGPATH + 256]; // temp location for created wal_diff

	ereport(DEBUG3,
			(errmsg("archiving \"%s\" via WAL-diff", file)));

	snprintf(wal_destination, MAXPGPATH, "%s/%s", wal_directory, file);
	snprintf(wal_diff_destination, MAXPGPATH, "%s/%s", wal_diff_directory, file);

	if (!is_file_archived(wal_destination))
	{
		(void) durable_rename(path, wal_destination, ERROR);
	}

	if (!is_file_archived(wal_diff_destination))
	{
		generate_temp_file_name(temp);
		copy_file(path, temp);

		if (!create_wal_diff(temp))
		{

		}

		(void) durable_rename(temp, wal_diff_destination, ERROR);

		ereport(DEBUG3,
				(errmsg("created WAL-diff for file \"%s\"", file)));
	}

	return true;
}

static bool 
is_file_archived(char *file) {
	struct stat st;
}

static void
generate_temp_name(char *temp) {

}

static bool 
create_wal_diff()
{

}

static void 
wall_diff_shutdown(ArchiveModuleState *state)
{
	///
}
