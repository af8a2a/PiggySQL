use crate::catalog::ColumnRef;
use crate::expression::{BinaryOperator, ScalarExpression};
use crate::optimizer::core::pattern::Pattern;
use crate::optimizer::core::pattern::PatternChildrenPredicate;
use crate::optimizer::core::rule::Rule;
use crate::optimizer::heuristic::graph::{HepGraph, HepNodeId};
use crate::optimizer::OptimizerError;
use crate::planner::operator::filter::FilterOperator;
use crate::planner::operator::join::JoinType;
use crate::planner::operator::Operator;
use crate::types::LogicalType;
use itertools::Itertools;
use lazy_static::lazy_static;


lazy_static! {
    static ref PUSH_PREDICATE_INTO_SCAN: Pattern = {
        Pattern {
            predicate: |op| matches!(op, Operator::Filter(_)),
            children: PatternChildrenPredicate::Predicate(vec![Pattern {
                predicate: |op| matches!(op, Operator::Scan(_)),
                children: PatternChildrenPredicate::None,
            }]),
        }
    };
}

pub struct PushPredicateIntoScan;

impl Rule for PushPredicateIntoScan {
    fn pattern(&self) -> &Pattern {
        &PUSH_PREDICATE_INTO_SCAN
    }

    fn apply(&self, node_id: HepNodeId, graph: &mut HepGraph) -> Result<(), OptimizerError> {
        if let Operator::Filter(op) = graph.operator(node_id) {
            let child_id = graph.children_at(node_id)[0];
            if let Operator::Scan(child_op) = graph.operator(child_id) {
                if child_op.index_by.is_some() {
                    return Ok(());
                }

                //FIXME: now only support unique
                for meta in &child_op.index_metas {
                    let mut option = op.predicate.convert_binary(&meta.column_ids[0])?;

                    if let Some(mut binary) = option.take() {
                        binary.scope_aggregation()?;
                        let rearrange_binaries = binary.rearrange()?;

                        if rearrange_binaries.is_empty() {
                            continue;
                        }
                        let mut scan_by_index = child_op.clone();
                        scan_by_index.index_by = Some((meta.clone(), rearrange_binaries));

                        // The constant expression extracted in prewhere is used to
                        // reduce the data scanning range and cannot replace the role of Filter.
                        graph.replace_node(child_id, Operator::Scan(scan_by_index));

                        return Ok(());
                    }
                }
            }
        }

        Ok(())
    }
}