#include <stdlib.h>
#include <string.h>
#include "query.h"
#include "table.h"

database_query_t* database_query_init(database_query_type_t type,
                                      database_query_arg_t args) {
    database_query_t* query = (database_query_t*) malloc(sizeof(database_query_t));

    if ( query == NULL ) {
        return NULL;
    }

    query->type = type;
    query->args = args;

    return query;
}

database_table_t* database_query_execute_select(database_table_t* input,
                                                database_query_select_t* query) {
    database_tuple_t* raw_query = database_tuple_init(input->header->size);
    database_tuple_t* new_headers = database_tuple_dup(input->header);

    // Convert mapping into raw query
    for ( int i = 0; i < query->length; i++ ) {
        database_tuple_t* tuple = query->data[i];

        if ( tuple->size != 2 ) {
            return NULL;
        }

        // Get the key
        database_val_t* key = tuple->values[0];
        
        if ( key->type != DATABASE_STR ) {
            return NULL;
        }

        char* key_str = key->val.str;
        database_val_t* val = tuple->values[1];

        // Find the index to put it in
        for ( int j = 0; j < input->header->size; j++ ) {
            database_val_t* col = input->header->values[j];

            if ( strcmp(key_str, col->val.str) == 0 ) {
                if ( val->type != col->type ) {
                    database_tuple_clean(raw_query);
                    return NULL;
                }

                // Put in the data into the correct column
                raw_query->values[j] = database_val_dup(val);
                break;
            }
        }
    }

    // Fill in gaps with DATABASE_ANY
    for ( int i = 0; i < raw_query->size; i++ ) {
        if ( raw_query->values[i] == NULL ) {
            raw_query->values[i] = DB_ANY();
        }
    }

    database_tuple_vector_t* results = database_table_get(input, raw_query);
    database_tuple_clean(raw_query);

    if ( results == NULL ) {
        return NULL;
    }

    // Create a new primary key columns
    int* new_prim_keys = (int*) malloc(sizeof(int) * input->rows->nkeys);

    for ( int i = 0; i < input->rows->nkeys; i++ ) {
        new_prim_keys[i] = input->rows->keys[i];
    }

    // Create new table
    database_table_t* table = database_table_init("", new_headers, new_prim_keys, input->rows->nkeys);

    // Add new results to table
    for ( int i = 0; i < results->length; i++ ) {
        database_table_add(table, results->data[i]);
    }

    return table;
}

database_table_t* database_query_execute_project(database_table_t* input,
                                                 database_query_project_t* query) {
    return NULL;
}

database_table_t* database_query_execute_join(database_table_t* input,
                                              database_query_join_t* query) {
    return NULL;
}

database_table_t* database_query_execute(database_query_t* query,
                                         database_table_t* input) {
    switch ( query->type ) {
        case DB_QUERY_SELECT:  return database_query_execute_select(input, query->args.select);
        case DB_QUERY_PROJECT: return database_query_execute_project(input, query->args.project);
        case DB_QUERY_JOIN:    return database_query_execute_join(input, query->args.join);
    }
}

void database_query_clean(database_query_t* query) {
    free(query);
}