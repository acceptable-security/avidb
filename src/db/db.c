#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "db.h"
#include "util.h"

#define FILE_WRITE_STR(F, C) fwrite((C), sizeof(char), strlen((C)), (F))

#define DB_NAME_END    "\xFF"
#define DB_HEADERS_END "\xFE"
#define DB_COL_END     "\xFD"
#define DB_ROW_END     "\xFC"
#define DB_ROWS_END    "\xFB"

database_t* database_init(char* file_path) {
    database_t* db = (database_t*) malloc(sizeof(database_t));

    if ( db == NULL ) {
        return NULL;
    }

    db->table_list = NULL;
    db->file_path = file_path;
    db->file_data = NULL;

    return db;
}

void database_add_table(database_t* db, database_table_t* table) {
    table->next = db->table_list;
    db->table_list = table;
}

database_table_t* database_get_table(database_t* db, char* name) {
    database_table_t* curr = db->table_list;

    while ( curr != NULL ) {
        if ( strcmp(curr->name, name) == 0 ) {
            return curr;
        }

        curr = curr->next;
    }

    return NULL;
}

void database_save(database_t* db) {
    database_table_t* curr = db->table_list;

    FILE* file = fopen(db->file_path, "w");
    
    while ( curr != NULL ) {
        // Write table name
        FILE_WRITE_STR(file, curr->name);
        FILE_WRITE_STR(file, DB_NAME_END);

        // Write table headers
        int* primary_keys = curr->rows->keys;
        int primary_keys_count = curr->rows->nkeys;

        for ( int i = 0; i < curr->header->size; i++ ) {
            database_val_t* row = curr->header->values[i];

            FILE_WRITE_STR(file, row->val.str);
            FILE_WRITE_STR(file, "(");

            // Handle primary keys
            for ( int j = 0; j < primary_keys_count; j++ ) {
                if ( primary_keys[j] == i ) {
                    FILE_WRITE_STR(file, "*");
                    break;
                }
            }

            switch ( row->type ) {
                case DATABASE_UNUM: FILE_WRITE_STR(file, "UNUM"); break;
                case DATABASE_SNUM: FILE_WRITE_STR(file, "SNUM"); break;
                case DATABASE_STR:  FILE_WRITE_STR(file, "STR");  break;
                case DATABASE_DEC:  FILE_WRITE_STR(file, "DEC");  break;
                case DATABASE_ANY:  break;
            }

            FILE_WRITE_STR(file, ")");

            if ( i < (curr->header->size - 1) ) {
                FILE_WRITE_STR(file, ",");
            }
        }

        FILE_WRITE_STR(file, DB_HEADERS_END);

        database_tuple_vector_t* rows = database_table_get_all(curr);

        // Write table rows
        for ( int i = 0; i < rows->length; i++ ) {
            database_tuple_t* row = rows->data[i];

            if ( row == NULL ) {
                continue;
            }

            // Write the columns
            for ( int j = 0; j < row->size; j++ ) {
                database_val_t* val = row->values[j];

                // Write the values
                switch ( val->type ) {
                    case DATABASE_ANY:
                        break;

                    case DATABASE_STR:
                        fprintf(file, "%s", val->val.str);
                        break;

                    case DATABASE_SNUM:
                        fprintf(file, "%ld", val->val.snum);
                        break;

                    case DATABASE_UNUM:
                        fprintf(file, "%lu", val->val.unum);
                        break;
                    
                    case DATABASE_DEC:
                        fprintf(file, "%f", val->val.dec);
                        break;
                }

                if ( j < (row->size - 1) ) {
                    FILE_WRITE_STR(file, DB_COL_END);
                }
            }

            FILE_WRITE_STR(file, DB_ROW_END);
        }

        database_tuple_vector_clean(rows);
        FILE_WRITE_STR(file, DB_ROWS_END);
        curr = curr->next;
    }

    fclose(file);
}

void database_read_file(database_t* db) {
    FILE *file = fopen(db->file_path, "r");

    // Calculate size
    fseek(file, 0, SEEK_END);
    db->file_size = ftell(file);
    fseek(file, 0, SEEK_SET);

    db->file_data = (char *)malloc(db->file_size * sizeof(char));

    fread(db->file_data, sizeof(char), db->file_size, file);
    fclose(file);
}

int database_seek_until(database_t* db, int pos, char stop) {
    return seek_until(db->file_data, db->file_size, pos, stop);
}

database_val_type_t database_type_from_string(char* string) {
    if ( strcmp(string, "UNUM") == 0 ) return DATABASE_UNUM;
    if ( strcmp(string, "SNUM") == 0 ) return DATABASE_SNUM;
    if ( strcmp(string, "DEC")  == 0 ) return DATABASE_DEC;
    if ( strcmp(string, "STR")  == 0 ) return DATABASE_STR;
    return DATABASE_ANY;
}

database_tuple_t* database_parse_header(char* header, int size, int** keys, int* nkeys) {
    if ( size == 0 ) {
        return database_tuple_init(0);
    }

    int entries = 1;
    *nkeys = 0;

    // Count entries and primary keys
    for ( int i = 0; i < size; i++ ) {
        if ( header[i] == ',' ) {
            entries++;
        }

        if ( header[i] == '*' ) {
            (*nkeys)++;
        }
    }

    database_tuple_t* tuple = database_tuple_init(entries);
    *keys = (int*) malloc(sizeof(int) * (*nkeys));

    int pos = 0;
    int index = 0;
    int cur_key = 0;

    while ( pos < size ) {
        char* name = extract_until(header, size, &pos, "(");

        // Parse primary keys
        if ( header[pos] == '*' ) {
            printf("%s is a primary key\n", name);
            *keys[cur_key++] = index;
            pos++;
        }   
        
        // Parse type information
        char* type_string = extract_until(header, size, &pos, ")");
        database_val_type_t type = database_type_from_string(type_string);

        // Load into header tuple
        tuple->values[index++] = database_val_init(type, (database_val_val_t) name);

        if ( header[pos] == ',' ) {
            pos++;
        }
        else {
            break;
        }
    }

    if ( index != entries ) {
        database_tuple_clean(tuple);
        return NULL;
    }

    return tuple;
}

void database_parse_row(database_table_t* table, char* row_string, int size) {
    database_tuple_t* tuple = database_tuple_init(table->header->size);
    int pos = 0;

    for ( int i = 0; i < table->header->size && pos < size; i++ ) {
        database_val_type_t type = table->header->values[i]->type;
        char* str_value = extract_until(row_string, size, &pos, DB_COL_END);

        printf("row_str: %s\n", str_value);

        database_val_t* val = NULL;

        switch ( type ) {
            case DATABASE_STR:  val = DB_STR(str_value);        break;
            case DATABASE_UNUM: val = DB_UNUM(atol(str_value)); break;
            case DATABASE_SNUM: val = DB_SNUM(atol(str_value)); break;
            case DATABASE_DEC:  val = DB_DEC(atof(str_value));  break;
            case DATABASE_ANY:  break;
        }

        tuple->values[i] = val;
    }

    database_tuple_print(tuple);
    printf("\n");
    database_table_add(table, tuple);
}

void database_parse_rows(database_table_t* table, char* rows, int size) {
    int pos = 0;

    while ( pos < size ) {
        int tmp_pos = pos;
        char* row_string = extract_until(rows, size, &pos, DB_ROW_END);

        // Parse row
        database_parse_row(table, row_string, pos - tmp_pos - 1);
    }
}

database_t* database_load(char* file_path) {
    database_t* db = database_init(file_path);
    database_read_file(db);

    int pos = 0;

    printf("Loaded %d bytes\n", db->file_size);

    while ( pos < db->file_size ) {
        // File end
        if ( db->file_data[pos] == DB_ROWS_END[0] ) {
            break;
        }

        // Read table name
        char* table_name = extract_until(db->file_data, db->file_size, &pos, DB_NAME_END);
        printf("Reading %s\n", table_name);

        // Read table row
        int pre_pos = pos;
        char* header_line = extract_until(db->file_data, db->file_size, &pos, DB_HEADERS_END);

        int nkeys;
        int* keys;

        database_tuple_t* header = database_parse_header(header_line, pos - pre_pos - 1, &keys, &nkeys);
        database_table_t* table = database_table_init(table_name, header, keys, nkeys);

        // Read table columns
        pre_pos = pos;
        char* rows_string = extract_until(db->file_data, db->file_size, &pos, DB_ROWS_END);
        database_parse_rows(table, rows_string, pos - pre_pos - 1);

        database_add_table(db, table);
    }

    return db;
}

void database_dump(database_t* db) {
    database_table_t* curr = db->table_list;

    while ( curr != NULL ) {
        printf("Dumping %s...\n", curr->name);

        database_tuple_vector_t* output = database_table_get_all(curr);
        printf("%lu rows\n", output->length);

        for ( int i = 0; i < output->length; i++ ) {
            printf("- %d: ", i);
            database_tuple_print(output->data[i]);
            printf("\n");
        }

        database_tuple_vector_clean(output);

        curr = curr->next;
    }
}

void database_clean(database_t* db) {
    database_table_t* cur = db->table_list;

    while ( cur != NULL ) {
        database_table_t* tmp = cur->next;
        database_table_clean(cur);
        cur = tmp;
    }

    if ( db->file_data != NULL ) {
        free(db->file_data);
        db->file_data = NULL;
    }

    free(db);
}