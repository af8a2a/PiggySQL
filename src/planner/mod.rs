use crate::{
    catalog::TableName,
    optimizer::{
        heuristic::{batch::HepBatchStrategy, optimizer::HepOptimizer},
        rule::RuleImpl,
        OptimizerError,
    },
};

use self::operator::Operator;

pub mod operator;

#[derive(Debug, PartialEq, Clone)]
pub struct LogicalPlan {
    pub operator: Operator,
    pub childrens: Vec<LogicalPlan>,
}

impl LogicalPlan {
    pub fn child(&self, index: usize) -> Option<&LogicalPlan> {
        self.childrens.get(index)
    }

    pub fn referenced_table(&self) -> Vec<TableName> {
        fn collect_table(plan: &LogicalPlan, results: &mut Vec<TableName>) {
            if let Operator::Scan(op) = &plan.operator {
                results.push(op.table_name.clone());
            }
            for child in &plan.childrens {
                collect_table(child, results);
            }
        }

        let mut tables = Vec::new();
        collect_table(self, &mut tables);
        tables
    }
    pub fn explain(&self, indentation: usize) -> String {
        let mut result = format!("{:indent$}{}", "", self.operator, indent = indentation);


        for child in &self.childrens {
            result.push('\n');
            result.push_str(&child.explain(indentation + 2));
        }

        result
    }
}
