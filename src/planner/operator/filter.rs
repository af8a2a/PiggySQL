use super::Operator;
use crate::{expression::ScalarExpression, planner::LogicalPlan};
use std::fmt::Formatter;
use std::{fmt, vec};

#[derive(Debug, PartialEq, Clone)]
pub struct FilterOperator {
    pub predicate: ScalarExpression,
    pub having: bool,
}

impl FilterOperator {
    pub fn build(predicate: ScalarExpression, children: LogicalPlan, having: bool) -> LogicalPlan {
        LogicalPlan {
            operator: Operator::Filter(FilterOperator { predicate, having }),
            childrens: vec![children],
        }
    }
}

impl fmt::Display for FilterOperator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "Filter {}, Is Having: {}", self.predicate, self.having)?;

        Ok(())
    }
}
