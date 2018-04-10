char* _strdup(char* string);
char* substring(char* string, int start, int size);
int seek_until(char* data, int size, int pos, char stop);
char* extract_until(char* data, int size, int* pos, char* stop);