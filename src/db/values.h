#ifndef _VALUES_H
#define _VALUES_H

#include <stdint.h>
#include "util.h"

typedef uint32_t hash_t;

// Types of values
typedef enum {
    DATABASE_UNUM, // Unsigned number
    DATABASE_SNUM, // Signed number
    DATABASE_DEC,  // Double
    DATABASE_STR,  // String
    DATABASE_ANY   // Used in queries, implies *
} database_val_type_t;

// A union of values
typedef union {
    uint64_t unum;
    int64_t snum;
    double dec;
    char* str;
    void* none;
} database_val_val_t;

// Structure representing a value with type
typedef struct {
    database_val_type_t type;
    database_val_val_t val;
} database_val_t;


database_val_t* database_val_init(database_val_type_t type, database_val_val_t val);
database_val_t* database_val_dup(database_val_t* val);
void database_val_print(database_val_t* val);
int database_val_cmp(database_val_t* a, database_val_t* b);
hash_t database_val_hash(database_val_t* val);
void database_val_clean(database_val_t* val);

// Helper macros
#define DB_UNUM(X) database_val_init(DATABASE_UNUM, (database_val_val_t) (X))
#define DB_SNUM(X) database_val_init(DATABASE_SNUM, (database_val_val_t) (X))
#define DB_DEC(X)  database_val_init(DATABASE_DEC,  (database_val_val_t) (X))
#define DB_STR(X)  database_val_init(DATABASE_STR,  (database_val_val_t) (X))
#define DB_ANY()   database_val_init(DATABASE_ANY,  (database_val_val_t) (NULL))

#endif