SmartNIC-based Disaggregated Memory Database
========================================================================

This includes an implementation of a SmartNIC-based disaggregated memory database.
It uses a B+-tree index that is maintained in memory servers and cached in their SmartNICs.
Compute servers communicate with SmartNICs in memory servers with two-sided RDMA, and SmartNIC accesses host memory via one-sided RDMA.
This supports three concurrency control protocols in 2 phase-locking (2PL), i.e., NO\_WAIT, WAIT\_DIE, and WOUND\_WAIT.



## Directories ##
* `benchmark`: includes the implementation of YCSB benchmark and TPC-C benchmark.
* `client`: includes the implementation of compute server.
* `common`: includes general headers for the system.
* `concurrency`: includes the two concurrency control protocols.
* `index`: includes the B+-tree index implementation.
* `net`: contains RDMA networking implemetation for the connection between memory server and compute server.
* `server`: has the implementation of memory server.
* `storage`: contains DBMS tables.
* `worker`: includes the implementation of SmartNIC-based processing.
* `test`: contains benchmark tests.


## Build ##
Make sure to update the IP information of your memory servers in `host.txt` for SmartNIC to host connection in memory servers, and `dpu.txt` for compute servers to SmartNIC connection.
Also feel free to change parameters in `common/global.h` for different settings of workloads, transactions, and concurrency control protocols.

```sh
mkdir build && cd build
cmake .. && make -j
```
The build will create executable binary files for memory server (host and SmartNIC) and several compute server benchmarks.



## Run ##
Memory server hosts must be executed first, followed by memory server SmartNICs and compute servers.
SmartNIC binaries include two benchmarks, `ycsb_worker`, and `tpcc_worker`.
Compute server binaries include two benchmarks, `ycsb_compute`, and `tpcc_compute`.

```sh
# Examples
## run memory server
./memory_server

## YCSB SmartNIC and compute 
./ycsb_worker $workload_size 
./ycsb_compute --workload c --num 10000000 --threads 64 --zipfian 0.9 --latency

## TPC-C SmartNIC and compute 
./tpcc_worker
./tpcc_compute --threads 64 --latency
```
