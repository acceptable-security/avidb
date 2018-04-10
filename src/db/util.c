#include <string.h>
#include <stdlib.h>

char* _strdup(char* string) {
    int size = strlen(string) + 1;

    char* new_string = (char*) malloc(sizeof(char) * size);
    new_string[size - 1] = '\0';
    strcpy(new_string, string);

    return new_string;
}

char* substring(char* string, int start, int size) {
    char* new_string = (char*) malloc(sizeof(char) * size);
    new_string[size - 1] = '\0';
    memcpy(string, &string[start], size);

    return new_string;
}

int seek_until(char* data, int size, int pos, char stop) {
	for ( int i = pos; i < size; i++ ) {
		if ( data[i] == stop ) {
			return i;
		}
	}

	return size;
}

char* extract_until(char* data, int size, int* pos, char* stop) {
    int end = seek_until(data, size, *pos, stop[0]);
    char* value = substring(data, *pos, end - *pos);
    *pos = end + 1;

    return value;
}