use crate::{expression::ScalarExpression, planner::LogicalPlan};
mod column_pruning;
mod combine_operators;
mod constant_folder;
mod pushdown_limit;
mod pushdown_predicates;
mod simplification;

use self::{
    column_pruning::ColumnPruning, combine_operators::{CollapseProject, CombineFilter}, constant_folder::ConstantFolder, pushdown_limit::PushLimitIntoScan, pushdown_predicates::{PushPredicateIntoScan, PushPredicateThroughJoin}, simplification::SimplifyFilter
};

use super::{
    core::{pattern::Pattern, rule::Rule},
    heuristic::graph::{HepGraph, HepNodeId},
    OptimizerError,
};

#[derive(Debug, Copy, Clone)]
pub enum RuleImpl {
    ColumnPruning,
    // Combine operators
    CollapseProject,
    CombineFilter,
    SimplifyFilter,

    PushLimitIntoTableScan,
    // PushDown predicates
    PushPredicateIntoScan,
    PushPredicateThroughJoin,
    ConstantFolder,
}

impl Rule for RuleImpl {
    fn pattern(&self) -> &Pattern {
        match self {
            RuleImpl::ColumnPruning => ColumnPruning.pattern(),
            RuleImpl::PushLimitIntoTableScan => PushLimitIntoScan.pattern(),
            RuleImpl::PushPredicateIntoScan => PushPredicateIntoScan.pattern(),
            RuleImpl::ConstantFolder => ConstantFolder.pattern(),
            RuleImpl::PushPredicateThroughJoin => PushPredicateThroughJoin.pattern(),
            RuleImpl::CollapseProject => CollapseProject.pattern(),
            RuleImpl::CombineFilter => CombineFilter.pattern(),
            RuleImpl::SimplifyFilter => SimplifyFilter.pattern(),

            _ => unimplemented!(),
        }
    }

    fn apply(&self, node_id: HepNodeId, graph: &mut HepGraph) -> Result<(), OptimizerError> {
        match self {
            RuleImpl::ColumnPruning => ColumnPruning.apply(node_id, graph),
            RuleImpl::PushLimitIntoTableScan => PushLimitIntoScan.apply(node_id, graph),
            RuleImpl::PushPredicateIntoScan => PushPredicateIntoScan.apply(node_id, graph),
            RuleImpl::ConstantFolder => ConstantFolder.apply(node_id, graph),
            RuleImpl::PushPredicateThroughJoin => PushPredicateThroughJoin.apply(node_id, graph),
            RuleImpl::CollapseProject => CollapseProject.apply(node_id, graph),
            RuleImpl::CombineFilter => CombineFilter.apply(node_id, graph),
            RuleImpl::SimplifyFilter => SimplifyFilter.apply(node_id, graph),

            _ => unimplemented!(),
        }
    }
}

/// Return true when left is subset of right
pub fn is_subset_exprs(left: &[ScalarExpression], right: &[ScalarExpression]) -> bool {
    left.iter().all(|l| right.contains(l))
}