#ifndef _TABLE_H
#define _TABLE_H

#include <stdint.h>

#include "header.h"
#include "tuple.h"
#include "tuple_vector.h"
#include "hash_table.h"
#include "values.h"

#define TABLE_ROWS_GROWTH 32

struct database_table_s;
typedef struct database_table_s database_table_t;

struct database_table_s {
    database_table_t* next;
    char* name;

    database_header_t* header;
    database_hash_table_t* rows;
};

database_table_t* database_table_init(char* name,
                                      database_header_t* header,
                                      int* primary_keys,
                                      int primary_keys_count);

int database_table_get_header_id(database_table_t* table,
                                 database_val_t* val);

void database_table_add(database_table_t* table,
                        database_tuple_t* row);

database_tuple_vector_t* database_table_get(database_table_t* table,
                                            database_tuple_t* query);

void database_table_rem(database_table_t* table,
                        database_tuple_t* query);

database_tuple_vector_t* database_table_get_all(database_table_t* table);

void database_table_clean(database_table_t* table);

#endif