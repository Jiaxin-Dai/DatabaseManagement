
CFLAGS = -g -Wall#debug
CC = gcc
RM = /bin/rm -f
all: test_assign1_1

dberror.o :dberror.h dberror.c

storage_mgr.o: storage_mgr.h storage_mgr.c

test_assign1_1.o: test_helper.h test_assign1_1.c

test_assign1_1: dberror.o storage_mgr.o test_assign1_1.o

clean:
	${RM} *.o test_assign1_1