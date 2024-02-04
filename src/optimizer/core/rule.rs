use crate::optimizer::{
    heuristic::graph::{HepGraph, HepNodeId},
    OptimizerError,
};

use super::pattern::Pattern;

/// A rule is to transform logically equivalent expression
pub trait Rule {
    /// The pattern to determine whether the rule can be applied.
    fn pattern(&self) -> &Pattern;

    fn apply(&self, node_id: HepNodeId, graph: &mut HepGraph) -> Result<(), OptimizerError>;
}