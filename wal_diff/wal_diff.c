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

#include "archive/archive_module.h"
#include "storage/copydir.h"
// extern void copydir(const char *fromdir, const char *todir, bool recurse);
// extern void copy_file(const char *fromfile, const char *tofile);
#include "storage/fd.h"
// здесь функции для работы с файлами


// https://www.postgresql.org/docs/current/archive-modules.html


PG_MODULE_MAGIC;

static char *archive_directory = NULL;

// что-то типо такого
static void archive_startup(ArchiveModuleState *state);
static bool archive_configured(ArchiveModuleState *state);
static bool archive_file(ArchiveModuleState *state, const char *file, const char *path);
static bool compress_file(); // or create_wal_diff()
static void archive_compressed_file(); // or archive_wal_diff()
static void archive_shutdown(ArchiveModuleState *state)


// An archive library is loaded by dynamically loading a shared library 
// with the archive_library's name as the library base name.
static const ArchiveModuleCallbacks basic_archive_callbacks = {
    .startup_cb = archive_startup,                  // additional required initialization
	.check_configured_cb = archive_configured,      // whether the module id ready to work (is configured)
                                                    // the server will periodically call this function, 
                                                    // and archiving will proceed only when it returns true
	.archive_file_cb = archive_file,                // MUST HAVE -- archive single WAL file
	.shutdown_cb = archive_shutdown
};


void
_PG_init(void)
{
    //get archive_dir from GUC
}


// returns to Postgres archiving callbacks
const ArchiveModuleCallbacks *
_PG_archive_module_init(void)
{
	return &basic_archive_callbacks;
}
