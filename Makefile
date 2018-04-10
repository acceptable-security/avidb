#
# File: Makefile
# Creator: George Ferguson
# Modifed by: Avi Saven
# Created: Thu Jun 30 11:00:49 2016
# Time-stamp: <Sat Feb  3 23:22:20 EST 2018 asaven>
#

PROGRAMS = db

CFLAGS = -g -std=c99 -Wall -Werror

programs: $(PROGRAMS)

db: src/db/values.o \
	src/db/db.o \
	src/db/tuple.o \
	src/db/table.o \
	src/db/tuple_vector.o \
	src/db/hash_table.o \
	src/test.o \
	src/db/util.o
	$(CC) -o $@ $^

clean:
	-rm -f $(PROGRAMS) src/*.o src/db/*.o