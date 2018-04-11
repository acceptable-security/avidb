#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "query.h"
#include "table.h"
#include "util.h"

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
    int count = 0;
    int nkeys = 0;
    int* offsets = (int*) malloc(sizeof(int) * input->rows->nkeys);
    int* keys = (int*) malloc(sizeof(int) * input->rows->nkeys);

    // Count total keys and primary keys
    for ( int i = 0; i < query->size; i++ ) {
        database_val_t* query_val = query->values[i];

        for ( int j = 0; j < input->header->size; j++ ) {
            database_val_t* row = input->header->values[j];   

            if ( strcmp(row->val.str, query_val->val.str) == 0 ) {
                offsets[count++] = j;
                break;
            }
        }

        // Go through each of the predefined primary keys
        for ( int j = 0; j < input->rows->nkeys; j++ ) {
            // Get the primary key
            int nrow = input->rows->keys[j];
            database_val_t* row = input->header->values[nrow];

            // If we are a primary key, count us.
            if ( strcmp(row->val.str, query_val->val.str) == 0 ) {
                keys[nkeys++] = count - 1;
            }
        }
    }

    // Allocate and initialize the primary keys
    database_tuple_vector_t* results = database_table_get_all(input);

    if ( results == NULL ) {
        return NULL;
    }

    // Create new table
    database_table_t* table = database_table_init("", query, keys, nkeys);

    // Add new results to table
    for ( int i = 0; i < results->length; i++ ) {
        database_tuple_t* old_row = results->data[i];
        database_tuple_t* new_row = database_tuple_init(query->size);

        for ( int j = 0; j < count; j++ ) {
            new_row->values[j] = old_row->values[offsets[j]];
        }

        database_table_add(table, new_row);
    }

    return table;
}

database_table_t* database_query_execute_join(database_table_t* input,
                                              database_query_join_t* query) {
    database_table_t* input2 = query->src_table;
    database_tuple_vector_t* join_on = query->join_on;

    int columns = input->header->size + input2->header->size - join_on->length;
    int count = 0;

    database_tuple_t* new_header = database_tuple_init(columns);
    int nkeys = input->rows->nkeys;
    int* keys = (int*) malloc(sizeof(int) * count);

    // Load primary keys from first table
    for ( int i = 0; i < nkeys; i++ ) {
        keys[i] = input->rows->keys[i];
    }

    // Add headers from first input
    for ( int i = 0; i < input->header->size; i++ ) {
        database_val_t* head = input->header->values[i];
        char* a = _strdup(head->val.str);

        new_header->values[count++] = database_val_init(head->type, (database_val_val_t) a);
    }

    // Add headers that aren't mapped from second input
    for ( int i = 0; i < input2->header->size; i++ ) {
        int add = 1;
        database_val_t* head = input2->header->values[i];
        char* a = head->val.str;
        
        for ( int j = 0; j < join_on->length; j++ ) {
            database_tuple_t* pair = join_on->data[j];

            if ( pair->size != 2 ) {
                database_tuple_clean(new_header);
                return NULL;
            }

            char* key = pair->values[1]->val.str;

            if ( strcmp(a, key) == 0 ) {
                add = 0;
            }
        }

        if ( add == 1 ) {
            for ( int j = 0; j < input2->rows->nkeys; j++ ) {
                if ( input2->rows->keys[j] == i ) {
                    keys[nkeys++] = input2->rows->keys[j];
                }
            }

            new_header->values[count++] = database_val_init(head->type, (database_val_val_t) _strdup(a));
        }
    }

    database_table_t* output = database_table_init("", new_header, keys, nkeys);

    // Get all the rows input1 and input2
    database_tuple_vector_t* rows1 = database_table_get_all(input);
    database_tuple_vector_t* rows2 = database_table_get_all(input2);

    for ( int i = 0; i < rows1->length; i++ ) {
        database_tuple_t* row1 = rows1->data[i];

        printf("Checking ");
        database_tuple_print(row1);
        printf("\n");

        for ( int j = 0; j < rows2->length; j++ ) {
            database_tuple_t* row2 = rows2->data[j];
            int mismatch = 0;

            printf("against ");
            database_tuple_print(row2);
            printf("\n");

            // Merged row
            database_tuple_t* merge = database_tuple_init(new_header->size);
            int index = 0;

            // Add in rows1 
            for ( int k = 0; k < row1->size; k++ ) {
                merge->values[index++] = database_val_dup(row1->values[k]);
            }
            
            // Go through each column in row2
            for ( int k = 0; k < row2->size; k++ ) {
                database_val_t* key = NULL;
                database_val_t* val = NULL;

                // Go through each equivelance relation in join map
                for ( int l = 0; l < join_on->length; l++ ) {
                    database_tuple_t* pair = join_on->data[l];
                    key = pair->values[0];
                    val = pair->values[1];

                    // Get the corresponding columns
                    int col1 = database_table_get_header_id(input, key);
                    int col2 = database_table_get_header_id(input2, val);

                    // Skip this relation if it's not relevant to the
                    // current column from row2 we're checking
                    if ( col2 != k ) {
                        key = NULL;
                        val = NULL;
                        continue;
                    }

                    printf("Found %d %d\n", col1, col2);

                    // Get the values from row1 and row2
                    database_val_t* val1 = row1->values[col1];
                    database_val_t* val2 = row2->values[col2];

                    database_val_print(val1); printf("\n");
                    database_val_print(val2); printf("\n");

                    // If they don't match we are mismatched
                    if ( database_val_cmp(val1, val2) != 1 ) {
                        mismatch = 1;
                    }
 
                    break;
                }

                // If we mismatch then we found a pairing but failed to match
                // we then skip it entirely (skipping continued below)
                if ( mismatch == 1 ) {
                    break;
                }
                
                // No pairing and no mismatch, thus add the information
                if ( val == NULL && key == NULL ) {
                    merge->values[index++] = database_val_dup(row2->values[k]);
                }
            }

            // If we mismatch then we can't accept this row, so we skip
            if ( mismatch == 1 ) {
                continue;
            }

            // Add it
            database_table_add(output, merge);
        }
    }

    return output;
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