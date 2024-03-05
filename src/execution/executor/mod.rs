pub(crate) mod ddl;
pub(crate) mod dml;
pub(crate) mod dql;
pub(crate) mod set;
pub(crate) mod show;
use crate::{
    planner::{operator::Operator, LogicalPlan},
    storage::Transaction,
    types::tuple::Tuple,
};

use self::{
    ddl::{
        alter_table::{AddColumn, DropColumn},
        create_index::CreateIndex,
        create_table::CreateTable,
        drop_index::DropIndex,
        drop_table::DropTable,
    },
    dml::{copy::CopyFromFile, delete::Delete, insert::Insert, update::Update},
    dql::{
        agg::{hash_agg::HashAggExecutor, simple_agg::SimpleAggExecutor},
        describe::Describe,
        dummy::Dummy,
        explain::Explain,
        filter::Filter,
        index_scan::IndexScan,
        join::HashJoin,
        limit::Limit,
        projection::Projection,
        seq_scan::SeqScan,
        sort::Sort,
        values::Values,
    },
    set::SetVariable,
    show::ShowTables,
};
use crate::errors::*;

pub type Source = Result<Vec<Tuple>>;

pub trait Executor<T: Transaction> {
    fn execute(self, transaction: &mut T) -> Source;
}
pub fn build<T: Transaction>(plan: LogicalPlan, transaction: &mut T) -> Source {
    let LogicalPlan {
        operator,
        mut childrens,
        ..
    } = plan;

    match operator {
        Operator::Dummy => Dummy {}.execute(transaction),
        Operator::Aggregate(op) => {
            let input = childrens.pop().unwrap();
            // let input = build(childrens.remove(0), transaction);

            if op.groupby_exprs.is_empty() {
                SimpleAggExecutor::from((op, input)).execute(transaction)
            } else {
                HashAggExecutor::from((op, input)).execute(transaction)
            }
        }
        Operator::Filter(op) => {
            let input = childrens.pop().unwrap();

            // let input = build(childrens.remove(0), transaction);

            Filter::from((op, input)).execute(transaction)
        }
        Operator::Join(op) => {
            let right_input = childrens.pop().unwrap();
            let left_input = childrens.pop().unwrap();

            HashJoin::from((op, left_input, right_input)).execute(transaction)
        }
        Operator::Project(op) => {
            // let input = build(childrens.remove(0), transaction);
            let input = childrens.pop().unwrap();

            Projection::from((op, input)).execute(transaction)
        }
        Operator::Scan(op) => {
            if op.index_by.is_some() {
                // println!("build index scan");
                IndexScan::from(op).execute(transaction)
            } else {
                // println!("build seq scan");

                SeqScan::from(op).execute(transaction)
            }
        }
        Operator::Sort(op) => {
            let input = childrens.pop().unwrap();

            Sort::from((op, input)).execute(transaction)
        }
        Operator::Limit(op) => {
            let input = childrens.pop().unwrap();

            Limit::from((op, input)).execute(transaction)
        }
        Operator::Insert(op) => {
            let input = childrens.pop().unwrap();
            Insert::from((op, input)).execute(transaction)
        }
        Operator::Update(op) => {
            let input = childrens.pop().unwrap();
            // let values = build(childrens.remove(0), transaction);

            Update::from((op, input)).execute(transaction)
        }
        Operator::Delete(op) => {
            let input = childrens.pop().unwrap();

            Delete::from((op, input)).execute(transaction)
        }
        Operator::Values(op) => Values::from(op).execute(transaction),
        Operator::AddColumn(op) => {
            let input = childrens.pop().unwrap();
            AddColumn::from((op, input)).execute(transaction)
        }
        Operator::DropColumn(op) => {
            let input = childrens.pop().unwrap();
            DropColumn::from((op, input)).execute(transaction)
        }
        Operator::CreateTable(op) => CreateTable::from(op).execute(transaction),
        Operator::DropTable(op) => DropTable::from(op).execute(transaction),
        Operator::CreateIndex(op) => CreateIndex::from(op).execute(transaction),
        Operator::Explain => {
            let input = childrens.pop().unwrap();

            Explain::from(input).execute(transaction)
        }
        Operator::DropIndex(op) => DropIndex::from(op).execute(transaction),
        Operator::Show => ShowTables.execute(transaction),
        Operator::SetVar(op) => SetVariable::from(op).execute(transaction),
        Operator::CopyFromFile(op) => CopyFromFile::from(op).execute(transaction),
        Operator::Describe(op) => Describe::from(op).execute(transaction),
    }
}
