add_executable(server server.cpp)
target_link_libraries(server _server)

add_executable(client client.cpp)
target_link_libraries(client _client index net)
target_include_directories(client PUBLIC ${PROJECT_SOURCE_DIR}/index)

#add_executable(worker worker.cpp ${PROJECT_SOURCE_DIR}/system/worker.h ${IDX_SRC})
