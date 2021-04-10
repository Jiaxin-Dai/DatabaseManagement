  
cmake_minimum_required(VERSION 3.0)
#project name
project(assign3)

#add executable
add_executable(test_assign3_1 buffer_mgr.c buffer_mgr_stat.c dberror.c expr.c rm_serializer.c storage_mgr.c test_assign3_1.c record_mgr.c)
add_executable(test_expr buffer_mgr.c buffer_mgr_stat.c dberror.c expr.c rm_serializer.c storage_mgr.c record_mgr.c test_expr.c)