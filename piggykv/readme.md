PiggyKV
experiment use for [PiggySQL](https://github.com/af8a2a/PiggySQL)  
fork from [KipDB](https://github.com/KKould/KipDB)

write as a learn project of storage engine  
i need a stable storage engine to support PiggySQL  
in main-branch,the storage is based on ToyDB's design,use the thread-safe storage engine as backend storage,
and build a MVCC transaction layer on top of it.but it is very slow and stupid,each storage engine must 
build a DoubleEnditerator trait for MVCC....  
Bitcask is too simple, mini-lsm is too young for release,in memory engine will lost data(like crossbeam-skiplist's inner bug),but sled backend is very good.  
KipDB look like good,so i try to copy the code from KipDB and make some good feature on top of it(like  
column family,raft.....).  
