#pragma once
#include <time.h>

class Timer_t{
    public:
	Timer_t(): elapsed(0) { }

	void Start(){
	    clock_gettime(CLOCK_MONOTONIC, &start);
	}

	void Stop(){
	    clock_gettime(CLOCK_MONOTONIC, &end);
	    elapsed = (end.tv_sec - start.tv_sec) * 1000000000 + (end.tv_nsec - start.tv_nsec);
	}

	size_t Get(){
	    return elapsed;
	}


    private:
	struct timespec start, end;
	size_t elapsed;
};
