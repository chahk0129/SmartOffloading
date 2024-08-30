#include "net/config.h"

config_t::config_t(std::string data){
    std::ifstream ifs;
    ifs.open(data);

    if(!ifs.is_open()){
	debug::notify_error("Failed to open config file %s", data);
	exit(0);
    }

    ifs >> server_num;
    uint16_t id;
    std::string ip;
    for(int i=0; i<server_num; i++){
	ifs >> id >> ip;
	id2ip.insert(std::pair<uint16_t, std::string>(id, ip));
	ip2id.insert(std::pair<std::string, uint16_t>(ip, id));
    }

    ifs.close();
}

config_t::~config_t(){ }

std::string config_t::get_ip(uint16_t id){
    return id2ip[id];
}

uint16_t config_t::get_id(std::string ip){
    return ip2id[ip];
}

uint16_t config_t::get_server_num(){
    return server_num;
}

std::unordered_map<uint16_t, std::string> config_t::get_instance(){
    return id2ip;
}

