use crate::{
    optimizer::heuristic::optimizer::HepOptimizer, planner::LogicalPlan, types::errors::TypeError,
};

use self::{heuristic::batch::HepBatchStrategy, rule::RuleImpl};

mod core;
pub mod heuristic;
pub mod rule;

#[derive(thiserror::Error, Debug)]
pub enum OptimizerError {
    #[error("type error")]
    TypeError(
        #[source]
        #[from]
        TypeError,
    ),
}

pub fn apply_optimization(plan: LogicalPlan) -> Result<LogicalPlan, OptimizerError> {
    HepOptimizer::new(plan)
        // .batch(
        //     "Column Pruning".to_string(),
        //     HepBatchStrategy::once_topdown(),
        //     vec![RuleImpl::ColumnPruning],
        // )
        .batch(
            "Simplify Filter".to_string(),
            HepBatchStrategy::fix_point_topdown(10),
            vec![RuleImpl::SimplifyFilter, RuleImpl::ConstantFolder],
        )
        .batch(
            "Predicate Pushdown".to_string(),
            HepBatchStrategy::fix_point_topdown(10),
            vec![
                RuleImpl::PushPredicateThroughJoin,
                RuleImpl::PushPredicateIntoScan,
            ],
        )
        .batch(
            "Combine Operators".to_string(),
            HepBatchStrategy::fix_point_topdown(10),
            vec![RuleImpl::CollapseProject, RuleImpl::CombineFilter],
        )
        .batch(
            "Limit Pushdown".to_string(),
            HepBatchStrategy::fix_point_topdown(10),
            vec![RuleImpl::PushLimitIntoTableScan],
        )
        .find_best()
}
