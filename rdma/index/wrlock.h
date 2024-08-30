#pragma once
#include <atomic>

class wrlock_t{
    private:
	std::atomic<uint16_t> l;
	const static uint16_t UNLOCKED = 0;
	const static uint16_t LOCKED = 1;

    public:
	wrlock_t(): l(UNLOCKED){ }

	void wlock(){
	    while(true){
		while(l.load(std::memory_order_relaxed) != UNLOCKED);

		auto f = UNLOCKED;
		if(l.compare_exchange_strong(f, LOCKED))
		    break;
	    }
	}

	bool try_wrlock(){
	    if(l.load(std::memory_order_relaxed) != UNLOCKED)
		return false;

	    auto f = UNLOCKED;
	    return l.compare_exchange_strong(f, LOCKED);
	}

	void rlock(){
	    while(true){
		uint16_t v;
		while((v = l.load(std::memory_order_relaxed)) == LOCKED);

		uint16_t b = v+2;
		if(l.compare_exchange_strong(v, b))
		    break;
	    }
	}

	void wunlock(){
	    l.store(UNLOCKED, std::memory_order_release);
	}

	bool try_rlock(){
	restart:
	    auto v = l.load(std::memory_order_relaxed);
	    if(v == LOCKED)
		return false;

	    uint16_t b = v+2;
	    if(!l.compare_exchange_strong(v, b))
		goto restart;
	    return true;
	}

	void runlock(){
	    while(true){
		uint16_t v = l.load();
		uint16_t b = v - 2;

		if(l.compare_exchange_strong(v, b))
		    break;
	    }
	}
};

