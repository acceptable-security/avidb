#include <stdio.h>
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
    char* new_string = (char*) malloc(sizeof(char) * (size + 1));
    memcpy(new_string, &string[start], size);
    new_string[size] = '\0';
    
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
    int start = *pos;
    int end = seek_until(data, size, start, stop[0]);
    data[end] = '\0';
    
    *pos = end + 1;

    return &data[start];
}