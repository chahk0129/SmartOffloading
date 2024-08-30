#include "net/net.h"

void server_listen(struct server_client_meta* local, struct server_client_meta* remote){
    struct sockaddr_in addr;
    int sock;
    int on = 1;

    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(TCP_PORT);

    sock = socket(PF_INET, SOCK_STREAM, 0);
    if(sock < 0){
        debug::notify_error("Failed to create TCP socket");
        exit(0);
    }

    auto ret = setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(int));
    if(ret < 0){
        debug::notify_error("Failed to set sockopt");
        exit(0);
    }

    ret = bind(sock, (struct sockaddr*)&addr, sizeof(struct sockaddr));
    if(ret < 0){
        debug::notify_error("Failed to bind socket");
        exit(0);
    }

    listen(sock, 10);
    struct sockaddr_in remote_addr;
    socklen_t sin_size = sizeof(struct sockaddr_in);
    int fd;
    while(1 && (fd = accept(sock, (struct sockaddr*)&remote_addr, &sin_size)) != -1){
        if(!connect_qp(fd, local, remote)){
            debug::notify_error("Server failed to connect QP with worker");
            continue;
        }
        break;
    }
    close(sock);
}

void client_connect(const char* ip, struct server_client_meta* local, struct server_client_meta* remote){
    int sock = socket_connect(ip); // first connect to master server
    if(sock < 0){
        debug::notify_error("Failed to create TCP socket to server %s", ip);
        return;
    }

    if(!connect_qp(sock, local, remote)){
        debug::notify_error("Failed to connect QP with server %s", ip);
        return;
    }
    close(sock);
}


bool connect_qp(int sock, struct server_client_meta* local, struct server_client_meta* remote){
    if(socket_data_sync(sock, sizeof(struct server_client_meta), (char*)local, (char*)remote) < 0){
	debug::notify_error("Failed to exchange node metadata");
	return false;
    }
    return true;
}



int socket_data_sync(int sock, int size, char* local, char* remote){
    int ret = write(sock, local, size);
    if(ret < size)
        return -1;
    ret = 0;
    int total_bytes = 0;
    while(!ret && (total_bytes < size)){
        int read_bytes = read(sock, remote, size);
        if(read_bytes > 0)
            total_bytes += read_bytes;
        else
            ret = read_bytes;
    }
    return ret;
}

int socket_connect(const char* ip){
    struct sockaddr_in remote;
    int sock;
    struct timeval timeout = {3,0};
    memset(&remote, 0, sizeof(struct sockaddr_in));

    remote.sin_family = AF_INET;
    inet_aton(ip, (struct in_addr*)&remote.sin_addr);
    remote.sin_port = htons(TCP_PORT);
    debug::notify_info("Connecting to %s", ip);

    if((sock = socket(PF_INET, SOCK_STREAM, 0)) < 0){
        debug::notify_error("Connection to %s failed", ip);
        return -1;
    }

    int ret = setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, (const char*)&timeout, sizeof(struct timeval));
    if(ret < 0){
        debug::notify_error("Set timeout failed for TCP connection to %s", ip);
        return -1;
    }

    int t = 3;
    while(t >= 0 && connect(sock, (struct sockaddr*)&remote, sizeof(struct sockaddr)) < 0){
        debug::notify_error("Failed to connect to server %s", ip);
        t--;
        usleep(1000000);
    }

    if(t < 0)
        return -1;
    return sock;
}

