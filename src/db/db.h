#include "table.h"

typedef struct {
    char* file_path;
    char* file_data;
    int file_size;
    database_table_t* table_list;
} database_t;

database_t* database_init(char* file_path);
void database_add_table(database_t* db, database_table_t* table);
database_table_t* database_get_table(database_t* db, char* name);

void database_dump(database_t* db);
void database_save(database_t* db);
database_t* database_load(char* file_path);

void database_clean(database_t* db);