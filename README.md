Smart Offloading: Beyond RDMA for Disaggregated Memory Databases
========================================================================

This includes two research prototypes of disaggregated memory databases which work in a SmartNIC-based setting and a RDMA-based setting.
The system benchmark extends [DBx1000](https://github.com/yxymit/DBx1000), a multi-threaded in-memory DBMS.
The major changes are as follows:
* Changed the monolithic architecture of the DBMS to disaggregated memory architecture, which separates compute resources and memory resources.
* Added network support for RDMA communication.
* Replaced local hash index implementation with a distributed B+-tree implementation.
* Added WOUND\_WAIT protocol support.
* Extended its concurrency control protocols to RDMA-based and SmartNIC-based protocols. 


## Directories ##

* `rdma/`: includes the implementation of RDMA-based indexing and concurrency control.
* `smartnic/`: includes the implementation of SmartNIC-based indexing and concurrency control.



## Dependencies ##

* Mellanox OFED linux driver of v5.4-3.6.8.1 or higher for RDMA network setup.
* NVIDIA DOCA software of v.1.5.1 or higher for SmartNIC setup.
* CMake of v.2.8.5 or higher for compilation.
* Jemalloc allocator
* TBB library
* Hugepage setting

## Build ##

Each directory has its detailed instructions of how to build in its README file.
