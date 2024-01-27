use self::{
    aggregate::AggregateOperator, alter_table::{AddColumnOperator, DropColumnOperator}, create_table::CreateTableOperator, delete::DeleteOperator, drop_table::DropTableOperator, filter::FilterOperator, insert::InsertOperator, join::JoinOperator, limit::LimitOperator, project::ProjectOperator, scan::ScanOperator, sort::SortOperator, update::UpdateOperator, values::ValuesOperator
};

pub mod aggregate;
pub mod alter_table;
pub mod create_table;
pub mod delete;
pub mod drop_table;
pub mod filter;
pub mod insert;
pub mod join;
pub mod limit;
pub mod project;
pub mod scan;
pub mod sort;
pub mod update;
pub mod values;
#[derive(Debug, PartialEq, Clone)]
pub enum Operator {
    // DQL
    Dummy,
    Aggregate(AggregateOperator),
    Filter(FilterOperator),
    Join(JoinOperator),
    Project(ProjectOperator),
    Scan(ScanOperator),
    Sort(SortOperator),
    Limit(LimitOperator),
    Values(ValuesOperator),
    // DML
    Insert(InsertOperator),
    Update(UpdateOperator),
    Delete(DeleteOperator),
    // DDL
    AddColumn(AddColumnOperator),
    DropColumn(DropColumnOperator),
    CreateTable(CreateTableOperator),
    DropTable(DropTableOperator),
}
