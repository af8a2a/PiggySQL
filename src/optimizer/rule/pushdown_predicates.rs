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
    static ref PUSH_PREDICATE_THROUGH_JOIN: Pattern = {
        Pattern {
            predicate: |op| matches!(op, Operator::Filter(_)),
            children: PatternChildrenPredicate::Predicate(vec![Pattern {
                predicate: |op| matches!(op, Operator::Join(_)),
                children: PatternChildrenPredicate::None,
            }]),
        }
    };


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


fn split_conjunctive_predicates(expr: &ScalarExpression) -> Vec<ScalarExpression> {
    match expr {
        ScalarExpression::Binary {
            op: BinaryOperator::And,
            left_expr,
            right_expr,
            ..
        } => split_conjunctive_predicates(left_expr)
            .into_iter()
            .chain(split_conjunctive_predicates(right_expr))
            .collect_vec(),
        _ => vec![expr.clone()],
    }
}

/// reduce filters into a filter, and then build a new LogicalFilter node with input child.
/// if filters is empty, return the input child.
fn reduce_filters(filters: Vec<ScalarExpression>, having: bool) -> Option<FilterOperator> {
    filters
        .into_iter()
        .reduce(|a, b| ScalarExpression::Binary {
            op: BinaryOperator::And,
            left_expr: Box::new(a),
            right_expr: Box::new(b),
            ty: LogicalType::Boolean,
        })
        .map(|f| FilterOperator {
            predicate: f,
            having,
        })
}

/// Return true when left is subset of right, only compare table_id and column_id, so it's safe to
/// used for join output cols with nullable columns.
/// If left equals right, return true.
pub fn is_subset_cols(left: &[ColumnRef], right: &[ColumnRef]) -> bool {
    left.iter().all(|l| right.contains(l))
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


pub struct PushPredicateThroughJoin;

impl Rule for PushPredicateThroughJoin {
    fn pattern(&self) -> &Pattern {
        &PUSH_PREDICATE_THROUGH_JOIN
    }

    // TODO: pushdown_predicates need to consider output columns
    fn apply(&self, node_id: HepNodeId, graph: &mut HepGraph) -> Result<(), OptimizerError> {
        let child_id = graph.children_at(node_id)[0];
        if let Operator::Join(child_op) = graph.operator(child_id) {
            if !matches!(
                child_op.join_type,
                JoinType::Inner | JoinType::Left | JoinType::Right
            ) {
                return Ok(());
            }

            let join_childs = graph.children_at(child_id);
            let left_columns = graph.operator(join_childs[0]).referenced_columns(true);
            let right_columns = graph.operator(join_childs[1]).referenced_columns(true);

            let mut new_ops = (None, None, None);

            if let Operator::Filter(op) = graph.operator(node_id) {
                let filter_exprs = split_conjunctive_predicates(&op.predicate);

                let (left_filters, rest): (Vec<_>, Vec<_>) = filter_exprs
                    .into_iter()
                    .partition(|f| is_subset_cols(&f.referenced_columns(true), &left_columns));
                let (right_filters, common_filters): (Vec<_>, Vec<_>) = rest
                    .into_iter()
                    .partition(|f| is_subset_cols(&f.referenced_columns(true), &right_columns));

                let replace_filters = match child_op.join_type {
                    JoinType::Inner => {
                        if !left_filters.is_empty() {
                            if let Some(left_filter_op) = reduce_filters(left_filters, op.having) {
                                new_ops.0 = Some(Operator::Filter(left_filter_op));
                            }
                        }

                        if !right_filters.is_empty() {
                            if let Some(right_filter_op) = reduce_filters(right_filters, op.having)
                            {
                                new_ops.1 = Some(Operator::Filter(right_filter_op));
                            }
                        }

                        common_filters
                    }
                    JoinType::Left => {
                        if !left_filters.is_empty() {
                            if let Some(left_filter_op) = reduce_filters(left_filters, op.having) {
                                new_ops.0 = Some(Operator::Filter(left_filter_op));
                            }
                        }

                        common_filters
                            .into_iter()
                            .chain(right_filters)
                            .collect_vec()
                    }
                    JoinType::Right => {
                        if !right_filters.is_empty() {
                            if let Some(right_filter_op) = reduce_filters(right_filters, op.having)
                            {
                                new_ops.1 = Some(Operator::Filter(right_filter_op));
                            }
                        }

                        common_filters.into_iter().chain(left_filters).collect_vec()
                    }
                    _ => vec![],
                };

                if !replace_filters.is_empty() {
                    if let Some(replace_filter_op) = reduce_filters(replace_filters, op.having) {
                        new_ops.2 = Some(Operator::Filter(replace_filter_op));
                    }
                }
            }

            if let Some(left_op) = new_ops.0 {
                graph.add_node(child_id, Some(join_childs[0]), left_op);
            }

            if let Some(right_op) = new_ops.1 {
                graph.add_node(child_id, Some(join_childs[1]), right_op);
            }

            if let Some(common_op) = new_ops.2 {
                graph.replace_node(node_id, common_op);
            } else {
                graph.remove_node(node_id, false);
            }
        }

        Ok(())
    }
}