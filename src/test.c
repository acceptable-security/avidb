#include <stdio.h>
#include <string.h>

#include "school.h"
#include "db/tuple_vector.h"
#include "db/query.h"

void print_op(char* operation, database_tuple_t* vals) {
    printf("%s", operation);
    database_tuple_print(vals);
    printf(": ");
}

void print_clean(database_tuple_vector_t* vector) {
    database_tuple_vector_print(vector);
    printf("\n");
    database_tuple_vector_clean(vector);
}

void dump_table(database_table_t* table) {
    database_tuple_vector_t* output_rows = database_table_get_all(table);

    database_tuple_vector_print(output_rows);
    printf("\n");
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
    dump_table(cr);
    database_tuple_clean(query3);

    database_tuple_t* query4 = database_tuple(2,
        DB_STR("CS205"), DB_STR("CS120")
    );

    print_op("insert", query4);
    database_table_add(cp, query4);
    dump_table(cp);

    database_tuple_t* query5 = database_tuple(2,
        DB_STR("CS205"), DB_STR("CS101")
    );

    print_op("insert", query5);
    database_table_add(cp, query5);
    dump_table(cp);

    database_clean(db);
    printf("... basic operations completed\n\n");
}

void what_grade(char* name, char* class) {
    printf("Getting all the grades of %s in %s...\n", name, class);

    database_t* db = create_db();

    database_table_t* snap = database_get_table(db, "snap");
    database_table_t* csg = database_get_table(db, "csg");

    database_tuple_vector_t* snap_rows = database_table_get_all(snap);
    database_tuple_vector_t* csg_rows = database_table_get_all(csg);

    for ( int i = 0; i < snap_rows->length; i++ ) {
        database_tuple_t* snap_row = snap_rows->data[i];
        char* snap_row_name = snap_row->values[1]->val.str;
        uint64_t snap_row_id = snap_row->values[0]->val.unum;

        if ( strcmp(snap_row_name, name) == 0 ) {
            for ( int j = 0; j < csg_rows->length; j++ ) {
                database_tuple_t* csg_row = csg_rows->data[j];
                char* csg_row_class = csg_row->values[0]->val.str;
                uint64_t csg_row_id = csg_row->values[1]->val.unum;

                if ( snap_row_id == csg_row_id && strcmp(csg_row_class, class) == 0 ) {
                    printf("Found %s in %s for %s\n", csg_row->values[2]->val.str, class, name);
                }
            }
        }
    }

    database_tuple_vector_clean(snap_rows);
    database_tuple_vector_clean(csg_rows);
    database_clean(db);

    printf("... done!\n\n");
}

void where_when(char* name, char* hour, char* day) {
    printf("Finding where %s is at %s on %s\n", name, hour, day);

    database_t* db = create_db();

    database_table_t* snap = database_get_table(db, "snap");
    database_table_t* cr = database_get_table(db, "cr");
    database_table_t* cdh = database_get_table(db, "cdh");

    database_tuple_vector_t* snap_rows = database_table_get_all(snap);
    database_tuple_vector_t* cr_rows = database_table_get_all(cr);
    database_tuple_vector_t* cdh_rows = database_table_get_all(cdh);


    for ( int i = 0; i < cdh_rows->length; i++ ) {
        database_tuple_t* cdh_row = cdh_rows->data[i];
        char* cdh_row_course = cdh_row->values[0]->val.str;
        char* cdh_row_day = cdh_row->values[1]->val.str;
        char* cdh_row_hour = cdh_row->values[2]->val.str;

        if ( strcmp(cdh_row_day, day) == 0 && strcmp(cdh_row_hour, hour) == 0 ){
            for ( int j = 0; j < cr_rows->length; j++ ) {
                database_tuple_t* cr_row = cr_rows->data[j];
                char* cr_row_course = cr_row->values[0]->val.str;
                char* cr_row_room = cr_row->values[1]->val.str;

                if ( strcmp(cr_row_course, cdh_row_course) == 0 ) {
                    printf("Found %s in %s\n", name, cr_row_room);
                }
            }
        }
    }

    database_tuple_vector_clean(snap_rows);
    database_tuple_vector_clean(cdh_rows);
    database_tuple_vector_clean(cr_rows);
    database_clean(db);

    printf("... done!\n\n");
}

void select() {
    printf("Testing select...\n");
    database_t* db = create_db();
    database_table_t* csg = database_get_table(db, "csg");
    database_tuple_vector_t* args = database_tuple_vector_init(DB_VECTOR_OWNED, 8);

    database_tuple_vector_add(args, database_tuple(2,
        DB_STR("Course"), DB_STR("CS101")
    ));

    database_query_t* query = DB_SELECT(args);
    database_table_t* output_table = database_query_execute(query, csg);
        
    if ( output_table == NULL ) {
        printf("No result\n");
    }
    else {    
        dump_table(output_table);
        database_table_clean(output_table);
    }

    database_tuple_vector_clean(args);
    database_query_clean(query);
    database_clean(db);
    printf("... finished testing select.\n\n");
}

void project() {
    printf("Testing project...\n");

    database_t* db = create_db();
    database_table_t* csg = database_get_table(db, "csg");
    database_tuple_vector_t* args = database_tuple_vector_init(DB_VECTOR_OWNED, 8);

    database_tuple_vector_add(args, database_tuple(2,
        DB_STR("Course"), DB_STR("CS101")
    ));

    database_query_t* query = DB_SELECT(args);
    database_table_t* output_table = database_query_execute(query, csg);

    database_tuple_t* args2 = database_tuple(1,
        DB_UNUM("StudentId")
    );

    database_query_t* query2 = DB_PROJECT(args2);

    database_table_t* output_table2 = database_query_execute(query2, output_table);
  
    dump_table(output_table2);
    database_table_clean(output_table2);
    database_table_clean(output_table);
    database_tuple_clean(args2);
    database_tuple_vector_clean(args);
    database_query_clean(query);
    database_query_clean(query2);
    database_clean(db);

    printf("... finished testing project\n\n");
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
    database_clean(db);

    printf("... finished testing join\n\n");
}

void load() {
    printf("Testing loading a saved file...\n");

    printf("Saving student database...\n");
    database_t* db = create_db();
    database_save(db);
    database_clean(db);
    printf("... done\n");

    printf("Loading database from file...\n");
    database_t* db2 = database_load("test.db");
    database_dump(db2);
    database_clean(db2);
    printf("...\n");

    printf("... finished testing load\n\n");
}

void all() {
    printf("Testing all...\n");

    database_t* db = create_db();
    database_table_t* cdh = database_get_table(db, "cdh");
    database_table_t* cr = database_get_table(db, "cr");
    database_tuple_vector_t* args3 = database_tuple_vector_init(DB_VECTOR_OWNED, 8);

    database_tuple_vector_add(args3, database_tuple(2,
        DB_STR("Course"), DB_STR("Course")
    ));

    database_query_join_t test = (database_query_join_t) {
        .join_on = args3,
        .src_table = cr
    };

    database_query_t* query3 = DB_JOIN(&test);

    database_table_t* output_table3 = database_query_execute(query3, cdh);
    
    database_tuple_vector_t* args = database_tuple_vector_init(DB_VECTOR_OWNED, 8);

    database_tuple_vector_add(args, database_tuple(2,
        DB_STR("Room"), DB_STR("Turing Auditorium")
    ));

    database_query_t* query = DB_SELECT(args);
    database_table_t* output_table = database_query_execute(query, output_table3);

    database_tuple_t* args2 = database_tuple(2,
        DB_STR("Day"), DB_STR("Hour")
    );

    database_query_t* query2 = DB_PROJECT(args2);

    database_table_t* output_table2 = database_query_execute(query2, output_table);

    dump_table(output_table2);

    database_table_clean(output_table2);
    database_table_clean(output_table);
    database_tuple_clean(args2);
    database_tuple_vector_clean(args);
    database_query_clean(query);
    database_query_clean(query2);

    database_table_clean(output_table3);

    database_tuple_vector_clean(args3);
    database_query_clean(query3);
    database_clean(db);

    printf("... finished testing all\n\n");   
}

int main(int argc, char* argv[]) {
    basic_ops();
    what_grade("C. Brown", "CS101");
    where_when("C. Brown", "9AM", "W");
    select();
    project();
    join();
    all();
    load();

    return 0;
}