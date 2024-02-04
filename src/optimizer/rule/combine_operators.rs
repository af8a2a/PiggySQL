use crate::expression::{BinaryOperator, ScalarExpression};
use crate::optimizer::core::pattern::{Pattern, PatternChildrenPredicate};
use crate::optimizer::core::rule::Rule;
use crate::optimizer::heuristic::graph::{HepGraph, HepNodeId};
use crate::optimizer::OptimizerError;
use crate::planner::operator::filter::FilterOperator;
use crate::planner::operator::Operator;
use crate::types::LogicalType;
use lazy_static::lazy_static;

use super::is_subset_exprs;

lazy_static! {
    static ref COLLAPSE_PROJECT_RULE: Pattern = {
        Pattern {
            predicate: |op| matches!(op, Operator::Project(_)),
            children: PatternChildrenPredicate::Predicate(vec![Pattern {
                predicate: |op| matches!(op, Operator::Project(_)),
                children: PatternChildrenPredicate::None,
            }]),
        }
    };
    static ref COMBINE_FILTERS_RULE: Pattern = {
        Pattern {
            predicate: |op| matches!(op, Operator::Filter(_)),
            children: PatternChildrenPredicate::Predicate(vec![Pattern {
                predicate: |op| matches!(op, Operator::Filter(_)),
                children: PatternChildrenPredicate::None,
            }]),
        }
    };
}

/// Combine two adjacent project operators into one.
pub struct CollapseProject;

impl Rule for CollapseProject {
    fn pattern(&self) -> &Pattern {
        &COLLAPSE_PROJECT_RULE
    }

    fn apply(&self, node_id: HepNodeId, graph: &mut HepGraph) -> Result<(), OptimizerError> {
        if let Operator::Project(op) = graph.operator(node_id) {
            let child_id = graph.children_at(node_id)[0];
            if let Operator::Project(child_op) = graph.operator(child_id) {
                if is_subset_exprs(&op.exprs, &child_op.exprs) {
                    graph.remove_node(child_id, false);
                } else {
                    graph.remove_node(node_id, false);
                }
            }
        }

        Ok(())
    }
}

/// Combine two adjacent filter operators into one.
pub struct CombineFilter;

impl Rule for CombineFilter {
    fn pattern(&self) -> &Pattern {
        &COMBINE_FILTERS_RULE
    }

    fn apply(&self, node_id: HepNodeId, graph: &mut HepGraph) -> Result<(), OptimizerError> {
        if let Operator::Filter(op) = graph.operator(node_id) {
            let child_id = graph.children_at(node_id)[0];
            if let Operator::Filter(child_op) = graph.operator(child_id) {
                let new_filter_op = FilterOperator {
                    predicate: ScalarExpression::Binary {
                        op: BinaryOperator::And,
                        left_expr: Box::new(op.predicate.clone()),
                        right_expr: Box::new(child_op.predicate.clone()),
                        ty: LogicalType::Boolean,
                    },
                    having: op.having || child_op.having,
                };
                graph.replace_node(node_id, Operator::Filter(new_filter_op));
                graph.remove_node(child_id, false);
            }
        }

        Ok(())
    }
}
