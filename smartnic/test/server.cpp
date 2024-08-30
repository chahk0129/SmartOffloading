#include "common/global.h"
#include "server/server.h"
#include <chrono>

void run(server_t* server){
    while(1)
	sleep(UINT32_MAX);
}

int main(){
    auto server = new server_t();

    run(server);
    return 0;
}
