#ifndef _QUERY_H
#define _QUERY_H

#include "table.h"
#include "tuple.h"
#include "tuple_vector.h"

typedef enum {
    DB_QUERY_SELECT,
    DB_QUERY_PROJECT,
    DB_QUERY_JOIN
} database_query_type_t;

/* Select queries give arguments form of a map
 * where a map can be derived to a list of tuples (key, value)
 * thus this simple typedef works
 */
typedef database_tuple_vector_t database_query_select_t;

/* Project queries give arguments in the form of a list of
 * columns that you want to select, denotated by the names
 * in the headers. Thus a simple tuple works, where the arity
 * needs to be the same, but the undesired columns can be marked
 * with DATABASE_ANY
 */
typedef database_tuple_t database_query_project_t;

/* Join takes a source table, and list of columns that are
 * being joined on. Thus the input to the query operator
 * is the source table and a tuple vector where the arity
 * of each tuple is 2 (e.g. key value again) but the key
 * would be the column in the src table and the value is 
 * the column in the dest table.
 */
typedef struct {
    database_table_t* src_table;
    database_tuple_vector_t* join_on;
} database_query_join_t;

typedef union {
    database_query_select_t* select;
    database_query_project_t* project;
    database_query_join_t* join;
} database_query_arg_t;

typedef struct {
    database_query_type_t type;
    database_query_arg_t args;
} database_query_t;

database_query_t* database_query_init(database_query_type_t type,
                                      database_query_arg_t args);

database_table_t* database_query_execute(database_query_t* query,
                                         database_table_t* input);

void database_query_clean(database_query_t* query);

#define DB_PROJECT(X) database_query_init(DB_QUERY_PROJECT, (database_query_arg_t) (X))
#define DB_SELECT(X) database_query_init(DB_QUERY_SELECT, (database_query_arg_t) (X))
#define DB_JOIN(X) database_query_init(DB_QUERY_JOIN, (database_query_arg_t) (X))

#endif