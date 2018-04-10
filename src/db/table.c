#include <stdlib.h>
#include <string.h>
#include "table.h"

database_table_t* database_table_init(char* name, database_header_t* header) {
    database_table_t* table = (database_table_t*) malloc(sizeof(database_table_t));

    if ( table == NULL ) {
        return NULL;
    }

    table->name = strdup(name);
    table->header = header;
    table->rows = database_tuple_vector_init(DB_VECTOR_OWNED, 32);

    if ( table->rows == NULL ) {
        database_table_clean(table);
        return NULL;
    }

    return table;
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
            return -1;
        }
    }

    return 0;
}

int database_table_add(database_table_t* table,
                       database_tuple_t* row) {
    if ( database_table_correct(table, row) == -1 ) {
        return -1;
    }

    int row_id = database_tuple_vector_add(table->rows, row);

    // TODO - indices

    return row_id;
}

database_tuple_vector_t* database_table_get(database_table_t* table,
                                            database_tuple_t* query) {
    if ( database_table_correct(table, query) == -1 ) {
        return NULL;
    }

    database_tuple_vector_t* results = database_tuple_vector_init(DB_VECTOR_UNOWNED, 8);

    if ( results == NULL ) {
        return NULL;
    }

    // TODO - index lookup

    for ( int i = 0; i < table->rows->length; i++ ) {
        database_tuple_t* row = table->rows->data[i];

        if ( row == NULL ) {
            continue;
        }

        if ( database_tuple_cmp(query, row) == 1 ) {
            database_tuple_vector_add(results, row);
        }
    }

    return results;
}

uint64_t database_table_rem(database_table_t* table,
                            database_tuple_t* query) {
    uint64_t count = 0;

    if ( database_table_correct(table, query) == -1 ) {
        return count;
    }

    // TODO - index lookup
    
    for ( int i = 0; i < table->rows->length; i++ ) {
        database_tuple_t* row = table->rows->data[i];

        if ( database_tuple_cmp(query, row) == 1 ) {
            database_tuple_clean(row);
            table->rows->data[i] = NULL;

            count++;
        }
    }

    return count;
}

void database_table_clean(database_table_t* table) {
    if ( table->rows != NULL ) {
        database_tuple_vector_clean(table->rows);
        table->rows = NULL;
    }

    if ( table->name != NULL ) {
        free(table->name);
        table->name = NULL;
    }

    free(table);
}