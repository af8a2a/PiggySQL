use crate::{
    optimizer::{
        core::{
            pattern::{Pattern, PatternChildrenPredicate},
            rule::Rule,
        },
        heuristic::graph::{HepGraph, HepNodeId},
        OptimizerError,
    },
    planner::operator::{join::JoinCondition, Operator},
};
use lazy_static::lazy_static;

lazy_static! {
    static ref CONSTANT_CALCULATION_RULE: Pattern = {
        Pattern {
            predicate: |_| true,
            children: PatternChildrenPredicate::None,
        }
    };
}

#[derive(Copy, Clone)]
pub struct ConstantFolder;

impl ConstantFolder {
    fn _apply(node_id: HepNodeId, graph: &mut HepGraph) -> Result<(), OptimizerError> {
        let operator = graph.operator_mut(node_id);

        match operator {
            Operator::Aggregate(op) => {
                for expr in op.agg_calls.iter_mut().chain(op.groupby_exprs.iter_mut()) {
                    expr.constant_calculation()?;
                }
            }
            Operator::Filter(op) => {
                op.predicate.constant_calculation()?;
            }
            Operator::Join(op) => {
                if let JoinCondition::On { on, filter } = &mut op.on {
                    for (left_expr, right_expr) in on {
                        left_expr.constant_calculation()?;
                        right_expr.constant_calculation()?;
                    }
                    if let Some(expr) = filter {
                        expr.constant_calculation()?;
                    }
                }
            }
            Operator::Project(op) => {
                for expr in &mut op.exprs {
                    expr.constant_calculation()?;
                }
            }
            Operator::Scan(op) => {
                for expr in &mut op.columns {
                    expr.constant_calculation()?;
                }
            }
            Operator::Sort(op) => {
                for field in &mut op.sort_fields {
                    field.expr.constant_calculation()?;
                }
            }
            _ => (),
        }
        for child_id in graph.children_at(node_id) {
            Self::_apply(child_id, graph)?;
        }

        Ok(())
    }
}

impl Rule for ConstantFolder {
    fn pattern(&self) -> &Pattern {
        &Pattern {
            predicate: |_| true,
            children: PatternChildrenPredicate::None,
        }
    }

    fn apply(&self, node_id: HepNodeId, graph: &mut HepGraph) -> Result<(), OptimizerError> {
        Self::_apply(node_id, graph)?;
        // mark changed to skip this rule batch
        graph.version += 1;

        Ok(())
    }
}
