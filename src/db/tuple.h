#ifndef _TUPLE_H
#define _TUPLE_H

#include <stdint.h>
#include <stdarg.h>
#include "values.h"

typedef struct {
    uint64_t size;
    database_val_t** values;
} database_tuple_t;

database_tuple_t* database_tuple_init(uint64_t size);
database_tuple_t* database_tuple(int count, ...);
void database_tuple_print(database_tuple_t* tuple);
int database_tuple_cmp(database_tuple_t* a,
                       database_tuple_t* b);
int database_tuple_is_query(database_tuple_t* tuple);
hash_t database_tuple_hash(database_tuple_t* tuple);
void database_tuple_clean(database_tuple_t* tuple);
#endif