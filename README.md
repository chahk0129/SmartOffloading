Smart Offloading: Beyond RDMA for Disaggregated Memory Databases
========================================================================

This includes two implementations of disaggregated memory databases which work in a SmartNIC-based setting and a RDMA-based setting.
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


## Build ##

Each directory has its detailed instructions of how to build in its README file.
