#pragma once
#include <fstream>
#include <string>
#include <stdint.h>
#include <cstdlib>
#include <unordered_map>

#include "common/debug.h"

class config_t{
    public:
	config_t(std::string data);
	~config_t();

	std::string get_ip(uint16_t id);
	uint16_t get_id(std::string ip);

	uint16_t get_server_num();

	std::unordered_map<uint16_t, std::string> get_instance();

    private:
	uint16_t server_num;
	std::unordered_map<uint16_t, std::string> id2ip;
	std::unordered_map<std::string, uint16_t> ip2id;
};

