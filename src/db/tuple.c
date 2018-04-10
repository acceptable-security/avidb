#include <stdio.h>
#include <stdlib.h>
#include "tuple.h"

database_tuple_t* database_tuple_init(uint64_t size) {
    database_tuple_t* tuple = (database_tuple_t*) malloc(sizeof(database_tuple_t));

    if ( tuple == NULL ) {
        return NULL;
    }

    tuple->size = size;
    tuple->values = (database_val_t**) malloc(sizeof(database_val_t*) * tuple->size);

    if ( tuple->values == NULL ) {
        free(tuple);
        return NULL;
    }

    return tuple;
}

database_tuple_t* database_tuple(int count, ...) {
    database_tuple_t* tuple = database_tuple_init(count);

    va_list list;
    va_start(list, count);

    for ( int i = 0; i < count; i++ ) {
        tuple->values[i] = va_arg(list, database_val_t*);
    }

    va_end(list);

    return tuple;
}

int database_tuple_is_query(database_tuple_t* tuple) {
    for ( int i = 0; i < tuple->size; i++ ) {
        if ( tuple->values[i]->type == DATABASE_ANY ) {
            return 1;
        }
    }

    return 0;
}

void database_tuple_print(database_tuple_t* tuple) {
    printf("(");

    for ( int i = 0; i < tuple->size; i++ ) {
        database_val_print(tuple->values[i]);

        if ( i < (tuple->size - 1) ) {
            printf(", ");
        }
    }

    printf(")");
}

int database_tuple_cmp(database_tuple_t* a,
                       database_tuple_t* b) {
    if ( a->size != b->size ) {
        return 0;
    }

    for ( int i = 0; i < a->size; i++ ) {
        if ( database_val_cmp(a->values[i], b->values[i]) == 0 ) {
            return 0;
        }
    }

    return 1;
}

hash_t database_tuple_hash(database_tuple_t* tuple) {
    hash_t hash = 0;

    for ( int i =0; i < tuple->size; i++ ) {
        hash ^= (database_val_hash(tuple->values[i]) * 31);
    }

    return hash;
}

void database_tuple_clean(database_tuple_t* tuple) {
    if ( tuple->values == NULL ) {
        free(tuple->values);
        tuple->values = NULL;
    }

    free(tuple);
}