#include <stdio.h>

#include "school.h"
#include "db/tuple_vector.h"
#include "db/query.h"

int main(int argc, char* argv[]) {
    database_t* db = create_db();
    database_table_t* cdh = database_get_table(db, "cdh");
    
    database_tuple_vector_t* vector = database_tuple_vector_init(DB_VECTOR_OWNED, 8);
    
    database_tuple_vector_add(vector, database_tuple(2,
        DB_STR("Course"), DB_STR("CS101")
    ));

    database_query_t* query = DB_SELECT(vector);
    database_table_t* output_table = database_query_execute(query, cdh);
    database_tuple_vector_t* output_rows = database_table_get_all(output_table);

    database_tuple_vector_print(output_rows);
    printf("\n");

    database_save(db);
    database_clean(db);

    return 0;
}