#include <stdio.h>

#include "school.h"
#include "db/tuple_vector.h"
#include "db/query.h"

void select() {
    database_t* db = create_db();
    database_table_t* cdh = database_get_table(db, "cdh");
    database_tuple_vector_t* args = database_tuple_vector_init(DB_VECTOR_OWNED, 8);

    database_tuple_vector_add(args, database_tuple(2,
        DB_STR("Course"), DB_STR("CS101")
    ));

    database_query_t* query = DB_SELECT(args);
    database_table_t* output_table = database_query_execute(query, cdh);
        
    if ( output_table == NULL ) {
        printf("No result\n");
    }
    else {    
        database_tuple_vector_t* output_rows = database_table_get_all(output_table);

        database_tuple_vector_print(output_rows);
        printf("\n");
        database_tuple_vector_clean(output_rows);
        database_table_clean(output_table);
    }

    database_tuple_vector_clean(args);
    database_query_clean(query);
    database_save(db);
    database_clean(db);
}

void project() {
    database_t* db = create_db();
    database_table_t* cdh = database_get_table(db, "cdh");

    database_query_t* query = DB_PROJECT(database_tuple(1,
        DB_STR("Course")
    ));
    database_table_t* output_table = database_query_execute(query, cdh);
        
    if ( output_table == NULL ) {
        printf("No result\n");
    }
    else {    
        database_tuple_vector_t* output_rows = database_table_get_all(output_table);

        database_tuple_vector_print(output_rows);
        printf("\n");
        database_tuple_vector_clean(output_rows);
        database_table_clean(output_table);
    }

    database_query_clean(query);
    database_save(db);
    database_clean(db);
}

void join() {
    database_t* db = create_db();
    database_table_t* cdh = database_get_table(db, "cdh");
    database_table_t* cr = database_get_table(db, "cr");
    database_tuple_vector_t* args = database_tuple_vector_init(DB_VECTOR_OWNED, 8);

    database_tuple_vector_add(args, database_tuple(2,
        DB_STR("Course"), DB_STR("Course")
    ));

    database_query_join_t test = (database_query_join_t) {
        .join_on = args,
        .src_table = cr
    };

    database_query_t* query = DB_JOIN(&test);

    database_table_t* output_table = database_query_execute(query, cdh);
        
    if ( output_table == NULL ) {
        printf("No result\n");
    }
    else {    
        database_tuple_vector_t* output_rows = database_table_get_all(output_table);

        database_tuple_vector_print(output_rows);
        printf("\n");
        database_tuple_vector_clean(output_rows);
        database_table_clean(output_table);
    }

    database_tuple_vector_clean(args);
    database_query_clean(query);
    database_save(db);
    database_clean(db);
}

int main(int argc, char* argv[]) {
    select();
    project();
    join();

    return 0;
}