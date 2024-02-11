use crate::optimizer::core::pattern::Pattern;
use crate::optimizer::core::pattern::PatternChildrenPredicate;
use crate::optimizer::core::rule::Rule;
use crate::optimizer::heuristic::graph::{HepGraph, HepNodeId};

use crate::errors::*;

use crate::planner::operator::Operator;
use lazy_static::lazy_static;

lazy_static! {
    static ref PUSH_LIMIT_INTO_TABLE_SCAN_RULE: Pattern = {
        Pattern {
            predicate: |op| matches!(op, Operator::Limit(_)),
            children: PatternChildrenPredicate::Predicate(vec![Pattern {
                predicate: |op| matches!(op, Operator::Scan(_)),
                children: PatternChildrenPredicate::None,
            }]),
        }
    };
}
/// Push down `Limit` past a `Scan`.
pub struct PushLimitIntoScan;

impl Rule for PushLimitIntoScan {
    fn pattern(&self) -> &Pattern {
        &PUSH_LIMIT_INTO_TABLE_SCAN_RULE
    }

    fn apply(&self, node_id: HepNodeId, graph: &mut HepGraph) -> Result<()> {
        if let Operator::Limit(limit_op) = graph.operator(node_id) {
            let child_index = graph.children_at(node_id)[0];
            if let Operator::Scan(scan_op) = graph.operator(child_index) {
                let mut new_scan_op = scan_op.clone();

                new_scan_op.limit = (limit_op.offset, limit_op.limit);

                graph.remove_node(node_id, false);
                graph.replace_node(child_index, Operator::Scan(new_scan_op));
            }
        }

        Ok(())
    }
}
