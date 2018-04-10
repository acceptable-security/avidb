#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "hash_table.h"

database_hash_table_bucket_t* database_hash_table_bucket_init(database_hash_table_bucket_t* next_bucket,
                                                              database_tuple_t* tuple) {
    database_hash_table_bucket_t* bucket = (database_hash_table_bucket_t*) malloc(sizeof(database_hash_table_bucket_t));

    if ( bucket == NULL ) {
        return NULL;
    }

    bucket->prev_bucket = NULL;
    bucket->next_bucket = next_bucket;
    bucket->tuple = tuple;

    if ( next_bucket != NULL ) {
        next_bucket->prev_bucket = bucket;
    }

    return bucket;
}

void database_hash_table_bucket_search(database_hash_table_bucket_t* bucket,
                                       database_tuple_t* query,
                                       database_tuple_vector_t* output) {
    while ( bucket != NULL ) {
        if ( database_tuple_cmp(bucket->tuple, query) == 1 ) {
            database_tuple_vector_add(output, bucket->tuple);
        }

        bucket = bucket->next_bucket;
    }
}

void database_hash_table_bucket_remove_all(database_hash_table_bucket_t* bucket,
                                           database_hash_table_bucket_t** head,
                                           int primary,
                                           database_tuple_t* query) {
    while ( bucket != NULL ) {
        if ( database_tuple_cmp(bucket->tuple, query) == 1 ) {
            database_hash_table_bucket_remove(bucket, head, primary);
        }

        bucket = bucket->next_bucket;
    }
}

void database_hash_table_bucket_remove(database_hash_table_bucket_t* bucket,
                                       database_hash_table_bucket_t** head, int primary) {
    if ( bucket->prev_bucket != NULL ) {
        bucket->prev_bucket->next_bucket = bucket->next_bucket;
    }
    else {
        *head = NULL;
    }

    if ( bucket->next_bucket != NULL ) {
        bucket->next_bucket->prev_bucket = bucket->prev_bucket;
    }

    database_hash_table_bucket_clean(bucket, primary);
}

void database_hash_table_bucket_clean(database_hash_table_bucket_t* bucket, int primary) {
    if ( bucket->tuple != NULL && primary == 1 ) {
        database_tuple_clean(bucket->tuple);
    }

    bucket->tuple = NULL;

    free(bucket);
}

database_hash_table_t* database_hash_table_init(uint32_t table_size, int primary,
                                                int* keys, int nkeys) {
    database_hash_table_t* hash_table = (database_hash_table_t*) malloc(sizeof(database_hash_table_t));

    if ( hash_table == NULL ) {
        return NULL;
    }

    hash_table->primary = primary;
    hash_table->table_size = table_size;
    hash_table->keys = keys;
    hash_table->nkeys = nkeys;

    hash_table->table = (database_hash_table_bucket_t**) malloc(table_size * sizeof(database_hash_table_bucket_t*));

    if ( hash_table->table == NULL ) {
        database_hash_table_clean(hash_table);
        return NULL;
    }

    memset(hash_table->table, 0, table_size * sizeof(database_hash_table_bucket_t*));

    return hash_table;
}

int database_hash_table_compatible(database_hash_table_t* hash_table,
                                   database_tuple_t* query) {
    for ( int i = 0; i < hash_table->nkeys; i++ ) {
        int value_index = hash_table->keys[i];
        database_val_t* value = query->values[value_index];

        if ( value->type == DATABASE_ANY ) {
            return 0;
        }
    }

    return 1;
}

database_hash_table_bucket_t** database_hash_table_get_buckets(database_hash_table_t* hash_table,
                                                               database_tuple_t* query) {
    database_tuple_t* tmp = database_tuple_init(hash_table->nkeys);

    for ( int i = 0; i < hash_table->nkeys; i++ ) {
        int value_index = hash_table->keys[i];
        database_val_t* value = query->values[value_index];

        if ( value->type == DATABASE_ANY ) {
            return NULL;
        }

        tmp->values[i] = value;
    }

    hash_t hash = database_tuple_hash(tmp);

    uint32_t index = hash % hash_table->table_size;
    return &hash_table->table[index];
}

void database_hash_table_add(database_hash_table_t* hash_table,
                             database_tuple_t* tuple) {
    database_hash_table_bucket_t** buckets = database_hash_table_get_buckets(hash_table, tuple);
    *buckets = database_hash_table_bucket_init(*buckets, tuple);
}

database_tuple_vector_t* database_hash_table_get(database_hash_table_t* hash_table,
                                                 database_tuple_t* query) {
    database_hash_table_bucket_t** buckets = database_hash_table_get_buckets(hash_table, query); 
    database_tuple_vector_t* output = database_tuple_vector_init(DB_VECTOR_UNOWNED, 32);

    if ( buckets != NULL ) {
        database_hash_table_bucket_search(*buckets, query, output);
    }
    else {
        for ( int i = 0; i < hash_table->table_size; i++ ) {
            database_hash_table_bucket_t* bucket = hash_table->table[i];
            database_hash_table_bucket_search(bucket, query, output);
        }
    }

    return output;
}

void database_hash_table_rem(database_hash_table_t* hash_table,
                             database_tuple_t* query) {
    database_hash_table_bucket_t** buckets = database_hash_table_get_buckets(hash_table, query); 

    if ( buckets != NULL ) {
        database_hash_table_bucket_remove_all(*buckets, buckets, hash_table->primary, query);
    }
    else {
        for ( int i = 0; i < hash_table->table_size; i++ ) {
            database_hash_table_bucket_t* bucket = hash_table->table[i];
            database_hash_table_bucket_remove_all(bucket, &bucket, hash_table->primary, query);
        }
    }
}

void database_hash_table_clean(database_hash_table_t* hash_table) {
    if ( hash_table->keys != NULL ) { 
        free(hash_table->keys);
        hash_table->keys = NULL;
    }

    if ( hash_table->table != NULL ) {
        for ( int i = 0; i < hash_table->table_size; i++ ) {
            if ( hash_table->table[i] != NULL ) {
                database_hash_table_bucket_clean(hash_table->table[i], hash_table->primary);
                hash_table->table[i] = NULL;
            }
        }

        free(hash_table->table);
        hash_table->table = NULL;
    }

    free(hash_table);
}