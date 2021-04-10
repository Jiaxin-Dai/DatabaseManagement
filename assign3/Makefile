CFLAGS = -g -Wall#debug
CC = gcc
RM = /bin/rm -f
all: test_assign3_1 test_expr

dberror.o :dberror.h dberror.c

storage_mgr.o: storage_mgr.h storage_mgr.c

buffer_mgr.o: buffer_mgr.h buffer_mgr.c

buffer_mgr_stat.o: buffer_mgr_stat.c buffer_mgr_stat.h

expr.o: expr.c expr.h

record_mgr.o: record_mgr.h record_mgr.c

rm_serializer.o: dberror.h tables.h record_mgr.h rm_serializer.c

test_expr.o: test_expr.c

test_assign3_1.o: dberror.h  storage_mgr.h buffer_mgr.h buffer_mgr_stat.h test_helper.h record_mgr.h  tables.h test_assign3_1.c

test_assign3_1: dberror.o storage_mgr.o buffer_mgr.o buffer_mgr_stat.o record_mgr.o test_assign3_1.o rm_serializer.o expr.o

test_expr: dberror.o storage_mgr.o buffer_mgr.o buffer_mgr_stat.o record_mgr.o test_expr.o rm_serializer.o expr.o

clean:
	${RM} *.o test_assign3_1 test_expr