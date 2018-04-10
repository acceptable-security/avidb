#include <stdlib.h>
#include <stdio.h>
#include "db/db.h"

int* single_prim_key(int n) {
    int* keys = (int*) malloc(sizeof(int) * 1);
    keys[0] = n;
    return keys;
}

database_table_t* create_snap() {
    int* keys = single_prim_key(0);

    database_table_t* table = database_table_init("snap", database_tuple(4,
        DB_UNUM("StudentId"),
        DB_STR("Name"),
        DB_STR("Address"),
        DB_STR("Phone")
    ), keys, 1);

    database_table_add(table, database_tuple(4,
        DB_UNUM(12345ull),
        DB_STR("C. Brown"),
        DB_STR("12 Apple St."),
        DB_STR("555-1234")
    ));

    database_table_add(table, database_tuple(4,
        DB_UNUM(67890ull),
        DB_STR("L. Van Pelt"),
        DB_STR("34 Pear Ave."),
        DB_STR("555-5678")
    ));

    database_table_add(table, database_tuple(4,
        DB_UNUM(22222ull),
        DB_STR("P. Patty"),
        DB_STR("56 Grape Blvd."),
        DB_STR("555-9999")
    ));

    return table;
}

database_table_t* create_cp() {
    int* keys = single_prim_key(0);

    database_table_t* table = database_table_init("cp", database_tuple(
        2,
        DB_STR("Course"),
        DB_STR("Prerequisite")
    ), keys, 1);

    database_table_add(table, database_tuple(2,
        DB_STR("CS101"), DB_STR("CS100")
    ));

    database_table_add(table, database_tuple(2,
        DB_STR("EE200"), DB_STR("EE005")
    ));

    database_table_add(table, database_tuple(2,
        DB_STR("EE200"), DB_STR("CS100")
    ));

    database_table_add(table, database_tuple(2,
        DB_STR("CS120"), DB_STR("CS101")
    ));

    database_table_add(table, database_tuple(2,
        DB_STR("CS121"), DB_STR("CS120")
    ));

    database_table_add(table, database_tuple(2,
        DB_STR("CS205"), DB_STR("CS101")
    ));

    database_table_add(table, database_tuple(2,
        DB_STR("CS206"), DB_STR("CS121")
    ));

    database_table_add(table, database_tuple(2,
        DB_STR("CS206"), DB_STR("CS205")
    ));

    return table;
}

database_table_t* create_cdh() {
    int* keys = single_prim_key(0);

    database_table_t* table = database_table_init("cdh", database_tuple(
        3,
        DB_STR("Course"),
        DB_STR("Day"),
        DB_STR("Hour")
    ), keys, 1);

    database_table_add(table, database_tuple(3,
        DB_STR("CS101"), DB_STR("M"), DB_STR("9AM")
    ));

    database_table_add(table, database_tuple(3,
        DB_STR("CS101"), DB_STR("W"), DB_STR("9AM")
    ));

    database_table_add(table, database_tuple(3,
        DB_STR("CS101"), DB_STR("F"), DB_STR("9AM")
    ));

    database_table_add(table, database_tuple(3,
        DB_STR("EE200"), DB_STR("Tu"), DB_STR("10AM")
    ));

    database_table_add(table, database_tuple(3,
        DB_STR("EE200"), DB_STR("W"), DB_STR("1PM")
    ));

    database_table_add(table, database_tuple(3,
        DB_STR("EE200"), DB_STR("Th"), DB_STR("10AM")
    ));

    return table;
}

database_table_t* create_cr() {
    int* keys = single_prim_key(0);

    database_table_t* table = database_table_init("cr", database_tuple(
        2,
        DB_STR("Course"),
        DB_STR("Room")
    ), keys, 1);

    database_table_add(table, database_tuple(2,
        DB_STR("CS101"), DB_STR("Turing Auditorium")
    ));

    database_table_add(table, database_tuple(2,
        DB_STR("EE200"), DB_STR("25 Ohm Hall")
    ));

    database_table_add(table, database_tuple(2,
        DB_STR("PH100"), DB_STR("Newton Lab")
    ));

    return table;
}

database_t* create_db() {
    database_t* db = database_init("test.db");

    database_add_table(db, create_snap());
    database_add_table(db, create_cp());
    database_add_table(db, create_cdh());
    database_add_table(db, create_cr());

    return db;
}

int main(int argc, char* argv[]) {
    database_t* db = create_db();
    // database_table_t* cdh = database_get_table(db, "cdh");

    database_save(db);
    database_clean(db);

    return 0;
}