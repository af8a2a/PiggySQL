use crate::catalog::{ColumnRef, ColumnSummary};
use crate::errors::*;
use crate::expression::agg::Aggregate;
use crate::expression::ScalarExpression;
use crate::optimizer::core::pattern::{Pattern, PatternChildrenPredicate};
use crate::optimizer::core::rule::Rule;
use crate::optimizer::heuristic::graph::{HepGraph, HepNodeId};
use crate::planner::operator::Operator;
use crate::types::value::DataValue;
use crate::types::LogicalType;
use lazy_static::lazy_static;
use std::collections::HashSet;
use std::sync::Arc;

lazy_static! {
    static ref COLUMN_PRUNING_RULE: Pattern = {
        Pattern {
            predicate: |_| true,
            children: PatternChildrenPredicate::None,
        }
    };
}

macro_rules! trans_references {
    ($columns:expr) => {{
        let mut column_references = HashSet::with_capacity($columns.len());
        for column in $columns {
            column_references.insert(column.summary());
        }
        column_references
    }};
}

#[derive(Clone)]
pub struct ColumnPruning;

impl ColumnPruning {
    fn clear_exprs(column_references: &HashSet<&ColumnSummary>, exprs: &mut Vec<ScalarExpression>) {
        exprs.retain(|expr| {
            if column_references.contains(expr.output_columns().summary()) {
                return true;
            }
            expr.referenced_columns(false)
                .iter()
                .any(|column| column_references.contains(column.summary()))
        })
    }

    fn _apply(
        column_references: HashSet<&ColumnSummary>,
        all_referenced: bool,
        node_id: HepNodeId,
        graph: &mut HepGraph,
    ) {
        let operator = graph.operator_mut(node_id);

        match operator {
            Operator::Aggregate(op) => {
                //对于 Aggregate 操作，清除未引用的聚合表达式，并处理 COUNT(*) 的特殊情况。
                if !all_referenced {
                    Self::clear_exprs(&column_references, &mut op.agg_calls);

                    if op.agg_calls.is_empty() && op.groupby_exprs.is_empty() {
                        let value = Arc::new(DataValue::Utf8(Some("*".to_string())));
                        // only single COUNT(*) is not depend on any column
                        // removed all expressions from the aggregate: push a COUNT(*)
                        op.agg_calls.push(ScalarExpression::AggCall {
                            distinct: false,
                            kind: Aggregate::Count,
                            args: vec![ScalarExpression::Constant(value)],
                            ty: LogicalType::Integer,
                        })
                    }
                }
                let is_distinct = op.is_distinct;
                let referenced_columns = operator.referenced_columns(false);
                let mut new_column_references = trans_references!(&referenced_columns);
                // on distinct
                if is_distinct {
                    for summary in column_references {
                        new_column_references.insert(summary);
                    }
                }

                Self::recollect_apply(new_column_references, false, node_id, graph);
            }
            Operator::Project(op) => {
                //对于 Project 操作，清除未引用的投影表达式。

                let has_count_star = op.exprs.iter().any(ScalarExpression::has_count_star);
                if !has_count_star {
                    if !all_referenced {
                        Self::clear_exprs(&column_references, &mut op.exprs);
                    }
                    let referenced_columns = operator.referenced_columns(false);
                    let new_column_references = trans_references!(&referenced_columns);

                    Self::recollect_apply(new_column_references, false, node_id, graph);
                }
            }
            Operator::Sort(_) | Operator::Limit(_) | Operator::Join(_) | Operator::Filter(_) => {
                let temp_columns = operator.referenced_columns(false);
                let mut column_references = column_references;
                for column in temp_columns.iter() {
                    column_references.insert(column.summary());
                }
                for child_id in graph.children_at(node_id) {
                    let copy_references = column_references.clone();

                    Self::_apply(copy_references, all_referenced, child_id, graph);
                }
            }
            Operator::Scan(op) => {
                if !all_referenced {
                    Self::clear_exprs(&column_references, &mut op.columns);
                }
            }

            // Last Operator
            Operator::Dummy | Operator::Values(_) => (),
            // DDL Based on Other Plan
            Operator::Insert(_) | Operator::Update(_) | Operator::Delete(_) => {
                let referenced_columns = operator.referenced_columns(false);
                let new_column_references = trans_references!(&referenced_columns);

                if let Some(child_id) = graph.eldest_child_at(node_id) {
                    Self::recollect_apply(new_column_references, true, child_id, graph);
                } else {
                    unreachable!();
                }
            }
            // DDL Single Plan
            Operator::Explain => {
                if let Some(child_id) = graph.eldest_child_at(node_id) {
                    Self::_apply(column_references, true, child_id, graph);
                } else {
                    unreachable!()
                }
            }

            _ => (),
        }
    }

    fn recollect_apply(
        referenced_columns: HashSet<&ColumnSummary>,
        all_referenced: bool,
        node_id: HepNodeId,
        graph: &mut HepGraph,
    ) {
        for child_id in graph.children_at(node_id){
            let copy_references: HashSet<&ColumnSummary> = referenced_columns.clone();

            Self::_apply(copy_references, all_referenced, child_id, graph);
        }

        // for child_id in graph.children_at(node_id) {
        //     let new_references: HashSet<&ColumnSummary> = referenced_columns
        //         .iter()
        //         .map(|column| column.summary())
        //         .collect();

        //     Self::_apply(new_references, all_referenced, child_id, graph);
        // }
    }
}

impl Rule for ColumnPruning {
    fn pattern(&self) -> &Pattern {
        &COLUMN_PRUNING_RULE
    }

    fn apply(&self, node_id: HepNodeId, graph: &mut HepGraph) -> Result<()> {
        Self::_apply(HashSet::new(), true, node_id, graph);
        // mark changed to skip this rule batch
        graph.version += 1;

        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use crate::binder::test::select_sql_run;
    use crate::errors::DatabaseError;
    use crate::optimizer::heuristic::batch::HepBatchStrategy;
    use crate::optimizer::heuristic::optimizer::HepOptimizer;
    use crate::optimizer::rule::RuleImpl;
    use crate::planner::operator::join::JoinCondition;
    use crate::planner::operator::Operator;

    #[tokio::test]
    async fn test_column_pruning() -> Result<(), DatabaseError> {
        let plan = select_sql_run("select c1, c3 from t1 left join t2 on c1 = c3").await?;

        let best_plan = HepOptimizer::new(plan.clone())
            .batch(
                "test_column_pruning".to_string(),
                HepBatchStrategy::once_topdown(),
                vec![RuleImpl::ColumnPruning],
            )
            .find_best()?;

        assert_eq!(best_plan.childrens.len(), 1);
        match best_plan.operator {
            Operator::Project(op) => {
                assert_eq!(op.exprs.len(), 2);
            }
            _ => unreachable!("Should be a project operator"),
        }
        match &best_plan.childrens[0].operator {
            Operator::Join(op) => match &op.on {
                JoinCondition::On { on, filter } => {
                    assert_eq!(on.len(), 1);
                    assert!(filter.is_none());
                }
                _ => unreachable!("Should be a on condition"),
            },
            _ => unreachable!("Should be a join operator"),
        }

        assert_eq!(best_plan.childrens[0].childrens.len(), 2);

        for grandson_plan in &best_plan.childrens[0].childrens {
            match &grandson_plan.operator {
                Operator::Scan(op) => {
                    assert_eq!(op.columns.len(), 1);
                }
                _ => unreachable!("Should be a scan operator"),
            }
        }

        Ok(())
    }
}
