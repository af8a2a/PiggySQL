pub(crate) mod ddl;
pub(crate) mod dml;
pub(crate) mod dql;
pub(crate) mod show;



use crate::{
    planner::{operator::Operator, LogicalPlan},
    storage::Transaction,
    types::tuple::Tuple,
};

use self::{
    ddl::{
        alter_table::{AddColumn, DropColumn}, create_index::CreateIndex, create_table::CreateTable, drop_index::DropIndex, drop_table::DropTable
    },
    dml::{delete::Delete, insert::Insert, update::Update},
    dql::{
        agg::{hash_agg::HashAggExecutor, simple_agg::SimpleAggExecutor}, dummy::Dummy, explain::Explain, filter::Filter, index_scan::IndexScan, join::HashJoin, limit::Limit, projection::Projection, seq_scan::SeqScan, sort::Sort, values::Values
    },
};

use super::ExecutorError;
pub type BoxedExecutor = Result<Vec<Tuple>, ExecutorError>;

pub trait Executor<T: Transaction> {
    fn execute(self, transaction: &mut T) -> BoxedExecutor;
}
pub fn build<T: Transaction>(plan: LogicalPlan, transaction: &mut T) -> BoxedExecutor {
    let LogicalPlan {
        operator,
        mut childrens,
    } = plan;

    match operator {
        Operator::Dummy => Dummy {}.execute(transaction),
        Operator::Aggregate(op) => {
            let input = build(childrens.remove(0), transaction);

            if op.groupby_exprs.is_empty() {
                SimpleAggExecutor::from((op, input)).execute(transaction)
            } else {
                HashAggExecutor::from((op, input)).execute(transaction)
            }
        }
        Operator::Filter(op) => {
            let input = build(childrens.remove(0), transaction);

            Filter::from((op, input)).execute(transaction)
        }
        Operator::Join(op) => {
            let left_input = build(childrens.remove(0), transaction);
            let right_input = build(childrens.remove(0), transaction);

            HashJoin::from((op, left_input, right_input)).execute(transaction)
        }
        Operator::Project(op) => {
            let input = build(childrens.remove(0), transaction);

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
            let input = build(childrens.remove(0), transaction);

            Sort::from((op, input)).execute(transaction)
        }
        Operator::Limit(op) => {
            let input = build(childrens.remove(0), transaction);

            Limit::from((op, input)).execute(transaction)
        }
        Operator::Insert(op) => {
            let input = build(childrens.remove(0), transaction);

            Insert::from((op, input)).execute(transaction)
        }
        Operator::Update(op) => {
            let input = build(childrens.remove(0), transaction);
            // let values = build(childrens.remove(0), transaction);

            Update::from((op, input)).execute(transaction)
        }
        Operator::Delete(op) => {
            let input = build(childrens.remove(0), transaction);

            Delete::from((op, input)).execute(transaction)
        }
        Operator::Values(op) => Values::from(op).execute(transaction),
        Operator::AddColumn(op) => {
            let input = build(childrens.remove(0), transaction);
            AddColumn::from((op, input)).execute(transaction)
        }
        Operator::DropColumn(op) => {
            let input = build(childrens.remove(0), transaction);
            DropColumn::from((op, input)).execute(transaction)
        }
        Operator::CreateTable(op) => CreateTable::from(op).execute(transaction),
        Operator::DropTable(op) => DropTable::from(op).execute(transaction),
        Operator::CreateIndex(op) => CreateIndex::from(op).execute(transaction),
        Operator::Explain => {
            let input = childrens.remove(0);

            Explain::from(input).execute(transaction)

        },
        Operator::DropIndex(op) => DropIndex::from(op).execute(transaction),
    }
}
