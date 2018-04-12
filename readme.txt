# Project 4

All code written by Avi Saven. Hashing algorithm is based on the widely recognized Java hashCode algorithm (using prime numbers to generate unique outputs). Special care has been put into this project to make sure that it is generic, and not specifically for the given tables, so that it can be used in a variety of situations.

## Files

The testing code is provided in the top level directory src/, while the code of the database is isolated in src/db/, for ease of usage. The src/school.c file provides the basic tables formatted in a database, while the src/test.c loads the information from src/school.c and has the tests necessary.

## Usage Instructions

Do not include the dollar sign in the instructions.

   $ make
   $ ./db

The output is a run of each test as described by the project description. Outputs of the lookup commands are printed as a list of tuples, and the delete/insert tests will print the resulting table (as a list of tuples) after the insertions are completed. For the specific queries formatted messages will appear. For selection, projection, joining, and the combination of the three, the described command is executed and the output is printed as list of tuples. For saving and loading the default database is loaded, saved, closed, and reopened from the saved file, and dumped into standard IO using a specially formatted message. Valgrind reports no errors and 0 bytes leaked.