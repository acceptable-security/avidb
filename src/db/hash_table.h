#ifndef _HASH_TABLE
#define _HASH_TABLE

#include "tuple.h"
#include "tuple_vector.h"

struct database_hash_table_bucket_s;
typedef struct database_hash_table_bucket_s database_hash_table_bucket_t;

struct database_hash_table_bucket_s {
    database_hash_table_bucket_t* prev_bucket;
    database_hash_table_bucket_t* next_bucket;
    database_tuple_t* tuple;
};

typedef struct {
    int primary;
    int* keys;
    int nkeys;
    database_hash_table_bucket_t** table;
    uint32_t table_size;
} database_hash_table_t;

database_hash_table_bucket_t* database_hash_table_bucket_init(database_hash_table_bucket_t* next_bucket,
                                                              database_tuple_t* tuple);
void database_hash_table_bucket_search(database_hash_table_bucket_t* bucket,
                                       database_tuple_t* query,
                                       database_tuple_vector_t* output);
void database_hash_table_bucket_remove_all(database_hash_table_bucket_t* bucket,
                                           database_hash_table_bucket_t** head,
                                           int primary,
                                           database_tuple_t* query);
void database_hash_table_bucket_remove(database_hash_table_bucket_t* bucket,
                                       database_hash_table_bucket_t** head, int primary);
void database_hash_table_bucket_clean(database_hash_table_bucket_t* bucket, int primary);

database_hash_table_t* database_hash_table_init(uint32_t table_size, int primary,
                                                int* keys, int nkeys);
int database_hash_table_compatible(database_hash_table_t* hash_table,
                                   database_tuple_t* query);

void database_hash_table_add(database_hash_table_t* hash_table,
                             database_tuple_t* tuple);
database_tuple_vector_t* database_hash_table_get(database_hash_table_t* hash_table,
                                                 database_tuple_t* query);
void database_hash_table_rem(database_hash_table_t* hash_table,
                             database_tuple_t* query);

void database_hash_table_clean(database_hash_table_t* hash_table);

#endif