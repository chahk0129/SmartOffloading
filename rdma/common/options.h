#pragma once
#include <string>
#include <iostream>
struct options_t{
    std::string workload = "load";
    uint64_t threads = 1;
    uint64_t num = 10000000;
    double zipfian = 0.0;
    bool latency = false;
};

std::ostream& operator<<(std::ostream& os, const options_t& opt){
    os << "Benchmark Options:\n"
#if WORKLOAD == YCSB
       << "\tWorkload type: " << opt.workload << "\n"
       << "\tWorkload size: " << opt.num << " records\n"
       << "\tNumber of Client Threads: " << opt.threads << "\n"
       << "\tZipfian Factor: " << opt.zipfian << "\n"
       << "\tEanble latency measurements: " << opt.latency << std::endl;
#else // TPCC
       << "\tNumber of Client Threads: " << opt.threads << "\n"
       << "\tEanble latency measurements: " << opt.latency << std::endl;
#endif
    return os;
}
