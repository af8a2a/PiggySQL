# PiggySQL - a relational database 
- DDL
  - [x] Create
    - [x] Create Table
    - [x] Create Index
  - [x] Drop
    - [x] Drop Table
    - [x] Drop Index
  - [x] Alter
    - [x] Add Column
    - [x] Drop Column
- DQL
  - [x] Select
  - [x] Where
  - [x] Distinct
  - [x] Aggregation: Count / Sum / Avg / Min / Max
  - [x] Subquery
  - [x] Join: Left Outer / Right Outer / Full Outer / Inner / Cross
  - [x] Group By
  - [x] Having
  - [x] Order By
  - [x] Limit
  - [x] Explain
  - [x] Join: Inner/Left/Right
  - [x] Alias
  - [x] SubQuery(from)
  - [x] Show tables
- DML
  - [x] Insert
  - [x] Update
  - [x] Delete
- IndexType
  - [x] Primary Key
  - [x] Unique Key
- Concurrency Control
  - [x] Transaction
    - [x] Begin
    - [x] Commit
    - [x] Rollback
  - [x] Isolation Level
    - [x] Snapshot Isolation
    - [x] Serializable Snapshot Isolation
  - [x] Multi-Version Concurrency Control  
- Optimization
  - [x] RBO
    - [x] Predicate Pushdown
    - [x] ConstFolder
    - [x] Combine Filter
    - [x] Coloumn Pruning
    - [x] Limit Pushdown  
    - [x] Combine operator  

- Execution
  - [x] Volcano
- Net
  - [x] PSQL Client  
  - [x] Server  
  - [x] JDBC Driver (only a little)

## References
We have referred to the following excellent open-source database projects and express our gratitude for their work
- [systemxlabs/bustubx](https://github.com/systemxlabs/bustubx)
- [duckdb/duckdb](https://github.com/duckdb/duckdb)
- [Fedomn/sqlrs](https://github.com/Fedomn/sqlrs): Main reference materials, Optimizer and Executor all refer to the design of sqlrs
- [KipData/KipSQL](https://github.com/KipData/KipSQL): Main reference materials,refer to a largenumber of SQL layer designs,Excellent Work!
- [erikgrinaker/ToyDB](https://github.com/erikgrinaker/toydb): Main reference MVCC Design,Encode and Decode,Storage Engine,Transaction, architecture refer the design of toydb
- [skyzh/mini-lsm](https://github.com/skyzh/mini-lsm) refer to the design of Storage Engine
- [YumingxuanGuo/featherdb](https://github.com/YumingxuanGuo/featherdb) reference serializable snapshot isolation

## Run PiggySQL
```bash
cargo run --release --bin piggysql
```
then run 
```bash
psql 
```
to enter the postgres client

also connect to the database with JDBC (experimental)