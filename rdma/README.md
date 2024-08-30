RDMA-based Disaggregated Memory Database
========================================================================

This includes an implementation of a RDMA-based disaggregated memory database.
It uses a B+-tree index that is distributed in memory servers, working with one-sided RDMA in compute servers.
This supports two concurrency control protocols in 2 phase-locking (2PL), i.e., NO\_WAIT and WAIT\_DIE, which also work with one-sided RDMA.
Note that this WAIT\_DIE is not starvation-free, but follows the way in [prior work](http://alchem.usc.edu/portal/static/download/rdma_tx.pdf) to reduce tail latency with priority consideration.



## Directories ##
* `benchmark`: includes the implementation of YCSB benchmark and TPC-C benchmark.
* `client`: includes the implementation of compute server.
* `common`: includes general headers for the system.
* `concurrency`: includes the two concurrency control protocols.
* `index`: includes the B+-tree index implementation.
* `net`: contains RDMA networking implemetation for the connection between memory server and compute server.
* `index`: includes the B+-tree index implementation.
* `server`: has the implementation of memory server.
* `storage`: contains DBMS tables.
* `system`: includes general implementation of transactions.
* `test`: contains benchmark tests.


## Build ##
Make sure to update the IP information of your memory servers in `host.txt`, which is used to create QP connections from compute servers to memory servers.
Also feel free to change parameters in `common/global.h` for different settings of workloads, transactions, and concurrency control protocols.

```sh
mkdir build && cd build
cmake .. && make -j
```
The build will create executable binary files for memory server and several compute server benchmarks.



## Run ##
Memory servers must be run before compute servers.
Compute server binaries include three benchmarks, `idx_test`, `ycsb_compute`, and `tpcc_compute`.

```sh
# Examples
## run memory server
./memory_server $workload_size

## index microbenchmark
./idx_test --workload c --num 10000000 --threads 64 --zipfian 0.9

## YCSB benchmark
./ycsb_compute --workload c --num 10000000 --threads 64 --zipfian 0.9 --latency

## TPC-C benchmark
./tpcc_compute --threads 64 --latency
```
