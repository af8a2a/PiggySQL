use crate::optimizer::core::pattern::{Pattern, PatternChildrenPredicate, PatternMatcher};
use crate::optimizer::heuristic::batch::HepMatchOrder;
use crate::optimizer::heuristic::graph::{HepGraph, HepNodeId};

/// Use pattern to determines which rule can be applied
pub struct HepMatcher<'a, 'b> {
    pattern: &'a Pattern,
    start_id: HepNodeId,
    graph: &'b HepGraph,
}

impl<'a, 'b> HepMatcher<'a, 'b> {
    pub(crate) fn new(pattern: &'a Pattern, start_id: HepNodeId, graph: &'b HepGraph) -> Self {
        Self {
            pattern,
            start_id,
            graph,
        }
    }
}

impl PatternMatcher for HepMatcher<'_, '_> {
    fn match_opt_expr(&self) -> bool {
        let op = self.graph.operator(self.start_id);
        // check the root node predicate
        if !(self.pattern.predicate)(op) {
            return false;
        }

        match &self.pattern.children {
            PatternChildrenPredicate::Recursive => {
                // check
                for node_id in self
                    .graph
                    .nodes_iter(HepMatchOrder::TopDown, Some(self.start_id))
                {
                    if !(self.pattern.predicate)(self.graph.operator(node_id)) {
                        return false;
                    }
                }
            }
            PatternChildrenPredicate::Predicate(patterns) => {
                for node_id in self.graph.children_at(self.start_id) {
                    for pattern in patterns {
                        if !HepMatcher::new(pattern, node_id, self.graph).match_opt_expr() {
                            return false;
                        }
                    }
                }
            }
            PatternChildrenPredicate::None => (),
        }

        true
    }
}
