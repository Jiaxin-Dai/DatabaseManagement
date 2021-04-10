  
cmake_minimum_required(VERSION 3.0)
#project name
project(assign2)

#add executable
add_executable(test_assign2 dberror.c storage_mgr.c test_assign2_1.c buffer_mgr.c buffer_mgr_stat.c)
