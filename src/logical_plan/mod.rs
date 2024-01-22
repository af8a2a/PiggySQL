use sqlparser::ast::Expr;


pub enum LogicalPlan {
    Insert,
    Delete,
    Update,
    Select,
    TableScan,
    IndexScan,
}
