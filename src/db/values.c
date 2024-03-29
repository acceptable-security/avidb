#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include "values.h"
#include "util.h"

database_val_t* database_val_init(database_val_type_t type, database_val_val_t val) {
    database_val_t* db_val = (database_val_t*) malloc(sizeof(database_val_t));

    if ( db_val == NULL ) {
        return NULL;
    }

    db_val->type = type;
    db_val->val = val;

    return db_val;
}

database_val_t* database_val_dup(database_val_t* val) {
    database_val_val_t new_val;

    // switch( val->type ) {
    //     case DATABASE_STR:
    //         new_val = (database_val_val_t) _strdup(val->val.str);
    //     default:
            new_val = val->val;
    // }

    return database_val_init(val->type, new_val);
}

void database_val_print(database_val_t* val) {
    switch ( val->type ) {
        case DATABASE_UNUM: printf("%lullu", val->val.unum); break;
        case DATABASE_SNUM: printf("%ld", val->val.snum); break;
        case DATABASE_STR: printf("\"%s\"", val->val.str); break;
        case DATABASE_DEC: printf("%f", val->val.dec); break;
        case DATABASE_ANY: printf("*"); break;
    }
}

int database_val_cmp(database_val_t* a, database_val_t* b) {
    // * matches all
    if ( a->type == DATABASE_ANY || b->type == DATABASE_ANY ) {
        return 1;
    }

    // Types match
    if ( a->type != b->type ) {
        return 0;
    }

    // Values match
    switch ( a->type ) {
        case DATABASE_UNUM:
        case DATABASE_SNUM:
        case DATABASE_DEC:
            return a->val.unum == b->val.unum;

        case DATABASE_STR:
            return strcmp(a->val.str, b->val.str) == 0;

        default:
            return 0;
    }
}

hash_t database_val_hash(database_val_t* val) {
    hash_t hash = 0;

    switch ( val->type ) {
        case DATABASE_STR:
            for ( int i = 0; i < strlen(val->val.str); i++ ) {
                hash += 31 * hash + val->val.str[0];
            }
            break;

        case DATABASE_UNUM:
        case DATABASE_SNUM:
        case DATABASE_DEC:
            hash = ((val->val.unum >>  0) & 0xFF) * (31 * 0) +
                   ((val->val.unum >>  8) & 0xFF) * (31 * 1) +
                   ((val->val.unum >> 16) & 0xFF) * (31 * 2) +
                   ((val->val.unum >> 24) & 0xFF) * (31 * 3) +
                   ((val->val.unum >> 32) & 0xFF) * (31 * 4) +
                   ((val->val.unum >> 40) & 0xFF) * (31 * 5) +
                   ((val->val.unum >> 48) & 0xFF) * (31 * 6) +
                   ((val->val.unum >> 56) & 0xFF) * (31 * 7);
            break;

        case DATABASE_ANY:
            break;
    }

    return hash;
}

void database_val_clean(database_val_t* val) {
    free(val);
}