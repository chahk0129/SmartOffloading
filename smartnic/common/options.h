#pragma once
#include <string>
#include <iostream>
struct options_t{
    std::string workload = "load";
    uint64_t threads = 1;
    uint64_t num = 10000000;
    bool latency = false;
    double zipfian = 0.0;
};

std::ostream& operator<<(std::ostream& os, const options_t& opt){
    os << "Benchmark Options:\n"
       << "\tWorkload type: " << opt.workload << "\n"
       << "\tWorkload size: " << opt.num << " records\n"
       << "\tNumber of Client Threads: " << opt.threads << "\n"
       << "\tZipfian Factor: " << opt.zipfian << "\n"
       << "\tMeasure latency: " << opt.latency << std::endl;
    return os;
}

