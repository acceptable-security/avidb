#include <stdlib.h>
#include <string.h>
#include "table.h"

database_table_t* database_table_init(char* name,
                                      database_header_t* header,
                                      int* primary_keys,
                                      int primary_keys_count) {
    database_table_t* table = (database_table_t*) malloc(sizeof(database_table_t));

    if ( table == NULL ) {
        return NULL;
    }

    table->name = strdup(name);
    table->header = header;
    table->rows = database_hash_table_init(32, 1, primary_keys, primary_keys_count);

    if ( table->rows == NULL ) {
        database_table_clean(table);
        return NULL;
    }

    return table;
}

int database_table_get_header_id(database_table_t* table,
                                 database_val_t* val) {
    for ( int i = 0; i < table->header->size; i++ ) {
        if ( strcmp(table->header->values[i]->val.str, val->val.str) == 0 ) {
            return i;
        }
    }

    return -1;
}

int database_table_correct(database_table_t* table,
                           database_tuple_t* row) {
    if ( table->header->size != row->size ) {
        return -1;
    }

    for ( int i = 0; i < row->size; i++ ) {
        int our_type = table->header->values[i]->type;
        int their_type = row->values[i]->type;

        if ( our_type != their_type && their_type != DATABASE_ANY ) {
            return 0;
        }
    }

    return 1;
}

void database_table_add(database_table_t* table,
                        database_tuple_t* row) {
    if ( database_table_correct(table, row) == 0 ) {
        return;
    }

    database_hash_table_add(table->rows, row);
}

database_tuple_vector_t* database_table_get(database_table_t* table,
                                            database_tuple_t* query) {
    if ( database_table_correct(table, query) == 0 ) {
        return NULL;
    }

    database_tuple_vector_t* results = database_tuple_vector_init(DB_VECTOR_UNOWNED, 8);

    if ( results == NULL ) {
        return NULL;
    }

    // TODO - secondary indices

    return database_hash_table_get(table->rows, query);
}

void database_table_rem(database_table_t* table,
                        database_tuple_t* query) {
    if ( database_table_correct(table, query) == 0 ) {
        return;
    }
    
    // TODO - secondary indices

    database_hash_table_rem(table->rows, query);
}

database_tuple_vector_t* database_table_get_all(database_table_t* table) {
    database_tuple_t* query = database_tuple_init(table->header->size);

    for ( int i = 0; i < query->size; i++ ) {
        query->values[i] = DB_ANY();
    }

    return database_table_get(table, query);
}

void database_table_clean(database_table_t* table) {
    if ( table->rows != NULL ) {
        database_hash_table_clean(table->rows);
        table->rows = NULL;
    }

    if ( table->name != NULL ) {
        free(table->name);
        table->name = NULL;
    }

    free(table);
}