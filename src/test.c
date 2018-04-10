#include "db/db.h"

void create_db() {
    database_table_t* table = database_table_init("snap", database_tuple(
        4,
        DB_DEC("StudentId"),
        DB_STR("Name"),
        DB_STR("Address"),
        DB_STR("Phone")
    ));

    database_table_add(table, database_tuple(
        4,
        DB_DEC((uint64_t) 12345),
        DB_STR("C. Brown"),
        DB_STR("12 Apple St."),
        DB_STR("555-1234")
    ));

    database_table_clean(table);
}

int main(int argc, char* argv[]) {
    create_db();
    return 0;
}