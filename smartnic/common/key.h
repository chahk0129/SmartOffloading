#pragma once
#include <cstring>
#include <string>

template <typename Key_t>
Key_t MIN_KEY;

template <typename Key_t>
Key_t MAX_KEY;

template <typename Key_t>
void set_key(){
    if constexpr(sizeof(Key_t) <= 8){
	MIN_KEY<Key_t> = std::numeric_limits<Key_t>::min();
	MAX_KEY<Key_t> = std::numeric_limits<Key_t>::max();
    }
    else{
	size_t key_size = sizeof(Key_t);
	uint8_t m[key_size];
	for(int i=0; i<key_size; i++)
	    memset(&m[i], 0xFF, sizeof(uint8_t));
	memset(MIN_KEY<Key_t>, 0, sizeof(Key_t));
	memcpy(MAX_KEY<Key_t>, m, sizeof(Key_t));
    }
}

template <std::size_t key_size>
class generic_key{
    public:
	char data[key_size];

	inline void set_key(std::string key){
	    memset(data, 0, key_size);
	    if(key.size() >= key_size){
		memcpy(data, key.c_str(), key_size-1);
		data[key_size-1] = '\0';
	    }
	    else{
		strcpy(data, key.c_str());
		data[key.size()] = '\0';
	    }
	}

	generic_key() { memset(data, 0, key_size); }
	generic_key(int) { memset(data, 0, key_size); }
	generic_key(const generic_key<key_size>& other) { memcpy(data, other.data, key_size); }

	inline generic_key& operator=(const generic_key<key_size>& other){
	    memcpy(data, other.data, key_size);
	    return *this;
	}

	inline bool operator<(const generic_key<key_size>& other) { return strcmp(data, other.data) < 0; }
	inline bool operator>(const generic_key<key_size>& other) { return strcmp(data, other.data) > 0; }
	inline bool operator==(const generic_key<key_size>& other) { return strcmp(data, other.data) == 0; }
	inline bool operator!=(const generic_key<key_size>& other) { return !(*this == other); }
	inline bool operator<=(const generic_key<key_size>& other) { return !(*this > other); }
	inline bool operator>=(const generic_key<key_size>& other) { return !(*this < other); }
};

