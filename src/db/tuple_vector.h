#include "tuple.h"

#define DB_RESULT_GROWTH 8

#define DB_VECTOR_UNOWNED 0
#define DB_VECTOR_OWNED 1

typedef struct {
    int ownership;
    int growth;

    database_tuple_t** data;
    uint64_t length;
    uint64_t size;
} database_tuple_vector_t;

database_tuple_vector_t* database_tuple_vector_init(int ownership, int growth);
int database_tuple_vector_add(database_tuple_vector_t* vector, database_tuple_t* tuple);
void database_tuple_vector_clean(database_tuple_vector_t* vector);