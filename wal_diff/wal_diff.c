/*-------------------------------------------------------------------------
 * 
 * Идея пока такая 
 * 
 * Сохраняем файл с логами во временное хранилище
 * Скидываем на диск сервера
 * Обрабатываем этот файл -- компрессуем wal записи
 * Скидываем на архив-диск теперь уже wal diff
 *
 * postgres/contrib/basic_archive
 * postgres/include/archive
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "archive/archive_module.h"

PG_MODULE_MAGIC;

static char *archive_directory = NULL;

static const ArchiveModuleCallbacks basic_archive_callbacks = {};

void
_PG_init(void)
{

}
