use crate::{
    catalog::TableName,
    expression::{agg::Aggregate, ScalarExpression},
    planner::operator::aggregate::AggregateOperator,
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
}

// impl LogicalPlan {
//     pub fn format(&self, mut indent: String, root: bool, last: bool) -> String {
//         let mut s = indent.clone();
//         if !last {
//             s += "├─ ";
//             indent += "│  "
//         } else if !root {
//             s += "└─ ";
//             indent += "   ";
//         }
//         match self.operator {
//             Operator::Dummy => unimplemented!(),
//             Operator::Aggregate(ref op) => {
//                 for agg in op.agg_calls.iter() {
//                     match agg {
//                         ScalarExpression::AggCall { kind, .. } => {
//                             s += match kind {
//                                 Aggregate::Avg => "avg",
//                                 Aggregate::Max => "max",
//                                 Aggregate::Min => "min",
//                                 Aggregate::Sum => "sum",
//                                 Aggregate::Count => "count",
//                             }
//                         }
//                         _ => unreachable!(),
//                     }
//                 }
                
//             }
//             Operator::Filter(filter) => {
//             },
//             Operator::Join(_) => todo!(),
//             Operator::Project(_) => todo!(),
//             Operator::Scan(_) => todo!(),
//             Operator::Sort(_) => todo!(),
//             Operator::Limit(_) => todo!(),
//             Operator::Values(_) => todo!(),
//             Operator::Insert(_) => todo!(),
//             Operator::Update(_) => todo!(),
//             Operator::Delete(_) => todo!(),
//             Operator::AddColumn(_) => todo!(),
//             Operator::DropColumn(_) => todo!(),
//             Operator::CreateTable(_) => todo!(),
//             Operator::DropTable(_) => todo!(),
//         };
//         s
//     }
// }
