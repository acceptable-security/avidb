#include <stdio.h>

#include "school.h"
#include "db/tuple_vector.h"
#include "db/query.h"

void print_op(char* operation, database_tuple_t* vals) {
    printf("%s", operation);
    database_tuple_print(vals);
    printf("\n");
}

void print_clean(database_tuple_vector_t* vector) {
    database_tuple_vector_print(vector);
    printf("\n\n");
    database_tuple_vector_clean(vector);
}

void dump_table(database_table_t* table) {
    database_tuple_vector_t* output_rows = database_table_get_all(table);

    database_tuple_vector_print(output_rows);
    printf("\n\n");
    database_tuple_vector_clean(output_rows);
}

void basic_ops() {
    printf("Testing basic operations...\n");
    database_t* db = create_db();

    database_table_t* csg = database_get_table(db, "csg");
    database_table_t* cp = database_get_table(db, "cp");
    database_table_t* cr = database_get_table(db, "cr");

    if ( csg == NULL || cp == NULL || cr == NULL ) {
        printf("Failed to load databases\n");
        return;
    }
    
    database_tuple_t* query1 = database_tuple(3,
        DB_STR("CS101"), DB_UNUM(12345ul), DB_ANY()
    );

    print_op("lookup", query1);
    print_clean(database_table_get(csg, query1));
    database_tuple_clean(query1);

    database_tuple_t* query2 = database_tuple(2,
        DB_STR("CS205"), DB_STR("CS120")
    );

    print_op("lookup", query2);
    print_clean(database_table_get(cp, query2));
    database_tuple_clean(query2);

    database_tuple_t* query3 = database_tuple(2,
        DB_STR("CS101"), DB_ANY()
    );

    print_op("delete", query3);
    database_table_rem(cr, query3);
    printf("Resulting table: ");
    dump_table(cr);
    database_tuple_clean(query3);

    database_tuple_t* query4 = database_tuple(2,
        DB_STR("CS205"), DB_STR("CS120")
    );

    print_op("insert", query4);
    database_table_add(cp, query4);
    printf("Resulting table: ");
    dump_table(cp);

    database_tuple_t* query5 = database_tuple(2,
        DB_STR("CS205"), DB_STR("CS101")
    );

    print_op("insert", query5);
    database_table_add(cp, query5);
    printf("Resulting table: ");
    dump_table(cp);

    database_clean(db);
    printf("... basic operations completed\n");
}

void select() {
    printf("Testing select...\n");
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
        dump_table(output_table);
        database_table_clean(output_table);
    }

    database_tuple_vector_clean(args);
    database_query_clean(query);
    database_save(db);
    database_clean(db);
    printf("... finished testing select.\n");
}

void project() {
    printf("Testing project...\n");

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
        dump_table(output_table);
        database_table_clean(output_table);
    }

    database_query_clean(query);
    database_save(db);
    database_clean(db);

    printf("... finished testing project\n");
}

void join() {
    printf("Testing join...\n");

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
        dump_table(output_table);
        database_table_clean(output_table);
    }

    database_tuple_vector_clean(args);
    database_query_clean(query);
    database_save(db);
    database_clean(db);

    printf("... finished testing join\n");
}

void load() {
    printf("Testing loading a saved file...\n");

    database_t* db = database_load("test.db");
    database_dump(db);
    database_clean(db);

    printf("... finished testing load\n");
}

int main(int argc, char* argv[]) {
    basic_ops();
    select();
    project();
    join();
    load();

    return 0;
}