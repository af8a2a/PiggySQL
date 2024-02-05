# PiggySQL - a relational database for l purpose
- DDL
  - [x] Create
    - [x] Create Table
  - [x] Drop
  - [x] Alter
- DQL
  - [x] Select
  - [x] Where
  - [ ] Distinct
  - [x] Aggregation: Count / Sum / Avg / Min / Max
  - [x] Subquery
  - [x] Join: Left Outer / Right Outer / Full Outer / Inner / Cross
  - [x] Group By
  - [x] Having
  - [x] Order By
  - [x] Limit
- DML
  - [x] Insert
  - [x] Update
  - [x] Delete
-Concurrency Control
  - [x] Transaction
    - [x] Begin
    - [x] Commit
    - [x] Rollback
  - [x] Isolation Level
    - [x] Snapshot Isolation
    - [ ] Serializable Snapshot Isolation
  - [x] Multi-Version Concurrency Control  
- Optimization
  - [x] Predicate Pushdown
  - [x] ConstFolder
  - [x] Combine Filter
  - [x] Coloumn Pruning
- Execution
  - [x] Volcano

## References
- [systemxlabs/bustubx](https://github.com/systemxlabs/bustubx)
- [duckdb/duckdb](https://github.com/duckdb/duckdb)
- [Fedomn/sqlrs](https://github.com/Fedomn/sqlrs): Main reference materials, Optimizer and Executor all refer to the design of sqlrs
- [KipData/KipSQL](https://github.com/KipData/KipSQL): Main reference Traits Design,DataType and Expression
- [erikgrinaker/ToyDB](https://github.com/erikgrinaker/toydb): Main reference MVCC Design,Encode and Decode,Storage Engine,Transaction, architecture refer the design of toydb
- [skyzh/mini-lsm](https://github.com/skyzh/mini-lsm)
