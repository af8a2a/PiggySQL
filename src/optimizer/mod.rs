use std::time::Instant;

use tracing::debug;

use crate::{optimizer::heuristic::optimizer::HepOptimizer, planner::LogicalPlan};

use self::{heuristic::batch::HepBatchStrategy, rule::RuleImpl};
use crate::errors::*;
mod core;
pub mod heuristic;
pub mod rule;

pub fn apply_optimization(plan: LogicalPlan) -> Result<LogicalPlan> {
    let before = Instant::now();
    let plan = HepOptimizer::new(plan)
        .batch(
            "Column Pruning".to_string(),
            HepBatchStrategy::once_topdown(),
            vec![RuleImpl::ColumnPruning],
        )
        .batch(
            "Simplify Filter".to_string(),
            HepBatchStrategy::fix_point_topdown(10),
            vec![
                RuleImpl::SimplifyFilter,
                RuleImpl::ConstantFolder,
                RuleImpl::CollapseProject,
            ],
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
            vec![
                RuleImpl::LimitProjectTranspose,
                RuleImpl::PushLimitThroughJoin,
                RuleImpl::PushLimitIntoTableScan,
                RuleImpl::EliminateLimits,
            ],
        )
        .find_best();
    let after = Instant::now();
    debug!(
        "apply optimization cost time : {} us",
        after.duration_since(before).as_micros()
    );
    plan
}
