#pragma once
#include "common/global.h"
#ifndef LOCKTABLE
#include "concurrency/row_lock.h"
#endif

class table_t;
class catalog_t;
class txn_man_t;
class worker_transport_t;
class Access;

class lock_t;

class row_t{
    public:
	row_t(){ }
	row_t(int table_idx, uint64_t row_id=0);
	void init(table_t* table, int pid, uint64_t row_id=0);

	const char* get_table_name(table_t* table);
	table_t* get_table();
	catalog_t* get_schema();
	int get_field_cnt(catalog_t* schema);
	int get_tuple_size(catalog_t* schema);
	int get_tuple_size();
	uint64_t get_row_id() { return row_id; }

	void copy(catalog_t* schema, row_t* src);
	void copy(catalog_t* schema, row_t* src, int idx);
	void copy(row_t* src);

	int get_table_idx() { return table_idx; }
	void set_primary_key(uint64_t key) { primary_key = key; }
	uint64_t get_primary_key() { return primary_key; }

	void set_value(catalog_t* schema, int id, int val);
	void set_value(catalog_t* schema, int id, void* ptr);
	void set_value_plain(catalog_t* schema, int id, void* ptr);
	void set_value(catalog_t* schema, int id, void* ptr, int size);
	void set_value(catalog_t* schema, const char* col_name, void* ptr);

	void get_value(catalog_t* schema, int id, void* ptr);
	char* get_value(catalog_t* schema, int id);
	char* get_value(catalog_t* schema, char* col_name);

//	void inc_value(int id, uint64_t value);
//	void dec_value(int id, uint64_t value);

	void set_data(char* data, int size);
	char* get_data();

	void free_row();

	#ifndef LOCKTABLE
	RC get_row(access_t type, txn_man_t* txn);
	RC get_row(access_t type, txn_man_t* txn, uint64_t row_addr, int tid, int pid);
	void return_row(access_t type, txn_man_t* txn, uint64_t row_addr, int tid, int pid);
	void return_row(access_t type);

	lock_t lock;
	#endif
	table_t* table;
	int pid;
	int table_idx;

    private:
	uint64_t primary_key;
	uint64_t row_id;

    public:
	char data[ROW_LENGTH];
};
