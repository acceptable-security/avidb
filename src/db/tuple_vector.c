#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "tuple_vector.h"

database_tuple_vector_t* database_tuple_vector_init(int ownership, int growth) {
    database_tuple_vector_t* vector = (database_tuple_vector_t*) malloc(sizeof(database_tuple_vector_t));

    if ( vector == NULL ) {
        return NULL;
    }

    vector->length = 0;
    vector->size = growth;
    vector->data = (database_tuple_t**) malloc(sizeof(database_tuple_t*) * vector->size);

    if ( vector->data == NULL ) {
        database_tuple_vector_clean(vector);
        return NULL;
    }

    memset(vector->data, 0, sizeof(database_tuple_t*) * vector->size);

    vector->growth = growth;
    vector->ownership = ownership;

    return vector;
}

void database_tuple_vector_print(database_tuple_vector_t* vector) {
    printf("[");

    for ( int i = 0; i < vector->length; i++ ) {
        database_tuple_print(vector->data[i]);

        if ( i < (vector->length - 1) ) {
            printf(", ");
        }
    }

    printf("]");
}

int database_tuple_vector_add(database_tuple_vector_t* vector,
                              database_tuple_t* tuple) {
    if ( vector->length + 1 >= vector->size ) {
        vector->size += vector->growth;
        database_tuple_t** tmp = realloc(vector->data, sizeof(database_tuple_t*) * vector->size);

        if ( tmp == NULL ) {
            database_tuple_vector_clean(vector);
            return -1;
        }

        vector->data = tmp;
    }

    int id = vector->length;
    vector->data[id] = tuple;
    vector->length++;

    return id;
}

void database_tuple_vector_clean(database_tuple_vector_t* vector) {
    if ( vector->data != NULL ) {
        if ( vector->ownership == DB_VECTOR_OWNED ) {
            for ( int i = 0; i < vector->length; i++ ) {
                if ( vector->data[i] != NULL ) {
                    database_tuple_clean(vector->data[i]);
                    vector->data[i] = NULL;
                }
            }			
        }

        free(vector->data);
    }

    free(vector);
}