use crate::planner::LogicalPlan;
mod column_pruning;

use self::column_pruning::ColumnPruning;

use super::{core::{pattern::Pattern, rule::Rule}, heuristic::graph::{HepGraph, HepNodeId}, OptimizerError};


#[derive(Debug, Copy, Clone)]
pub enum RuleImpl {
    ColumnPruning,
    PushLimitThroughJoin,
    PushLimitIntoTableScan,
    // PushDown predicates
    PushPredicateThroughJoin,
    // Tips: need to be used with `SimplifyFilter`
    PushPredicateIntoScan,
    // Simplification
    SimplifyFilter,
    ConstantCalculation,
}



impl Rule for RuleImpl {
    fn pattern(&self) -> &Pattern {
        match self {
            RuleImpl::ColumnPruning => ColumnPruning.pattern(),
            // RuleImpl::PushLimitThroughJoin => PushLimitThroughJoin.pattern(),
            // RuleImpl::PushLimitIntoTableScan => PushLimitIntoScan.pattern(),
            // RuleImpl::PushPredicateThroughJoin => PushPredicateThroughJoin.pattern(),
            // RuleImpl::PushPredicateIntoScan => PushPredicateIntoScan.pattern(),
            // RuleImpl::SimplifyFilter => SimplifyFilter.pattern(),
            // RuleImpl::ConstantCalculation => ConstantCalculation.pattern(),
            _=>unimplemented!()
        }
    }

    fn apply(&self, node_id: HepNodeId, graph: &mut HepGraph) -> Result<(), OptimizerError> {
        match self {
            RuleImpl::ColumnPruning => ColumnPruning.apply(node_id, graph),
            // RuleImpl::PushLimitThroughJoin => PushLimitThroughJoin.apply(node_id, graph),
            // RuleImpl::PushLimitIntoTableScan => PushLimitIntoScan.apply(node_id, graph),
            // RuleImpl::PushPredicateThroughJoin => PushPredicateThroughJoin.apply(node_id, graph),
            // RuleImpl::SimplifyFilter => SimplifyFilter.apply(node_id, graph),
            // RuleImpl::PushPredicateIntoScan => PushPredicateIntoScan.apply(node_id, graph),
            // RuleImpl::ConstantCalculation => ConstantCalculation.apply(node_id, graph),
            _=>unimplemented!()
        }
    }
}