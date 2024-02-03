use crate::planner::LogicalPlan;
mod column_pruning;
mod constant_folder;
mod pushdown_limit;
mod pushdown_predicates;

use self::{
    column_pruning::ColumnPruning, constant_folder::ConstantFolder, pushdown_limit::PushLimitIntoScan, pushdown_predicates::PushPredicateIntoScan
};

use super::{
    core::{pattern::Pattern, rule::Rule},
    heuristic::graph::{HepGraph, HepNodeId},
    OptimizerError,
};

#[derive(Debug, Copy, Clone)]
pub enum RuleImpl {
    ColumnPruning,
    PushLimitIntoTableScan,
    // PushDown predicates
    PushPredicateIntoScan,
    ConstantFolder,
}

impl Rule for RuleImpl {
    fn pattern(&self) -> &Pattern {
        match self {
            RuleImpl::ColumnPruning => ColumnPruning.pattern(),
            RuleImpl::PushLimitIntoTableScan => PushLimitIntoScan.pattern(),
            RuleImpl::PushPredicateIntoScan => PushPredicateIntoScan.pattern(),
            RuleImpl::ConstantFolder => ConstantFolder.pattern(),
            _ => unimplemented!(),
        }
    }

    fn apply(&self, node_id: HepNodeId, graph: &mut HepGraph) -> Result<(), OptimizerError> {
        match self {
            RuleImpl::ColumnPruning => ColumnPruning.apply(node_id, graph),
            RuleImpl::PushLimitIntoTableScan => PushLimitIntoScan.apply(node_id, graph),
            RuleImpl::PushPredicateIntoScan => PushPredicateIntoScan.apply(node_id, graph),
            RuleImpl::ConstantFolder => ConstantFolder.apply(node_id, graph),
            _ => unimplemented!(),
        }
    }
}
