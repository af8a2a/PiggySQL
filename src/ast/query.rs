use serde::{Deserialize, Serialize};
use sqlparser::ast::TableWithJoins;

use crate::types::{expression::Expr, operator::IndexOperator};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Query {
    pub body: SetExpr,
    pub order_by: Vec<OrderByExpr>,
    pub limit: Option<Expr>,
    pub offset: Option<Expr>,
}


#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Select {
    pub projection: Vec<SelectItem>,
    pub from: TableWithJoins,
    /// WHERE
    pub selection: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub having: Option<Expr>,
}


#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TableAlias {
    pub name: String,
    pub columns: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Values(pub Vec<Vec<Expr>>);


#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OrderByExpr {
    pub expr: Expr,
    pub asc: Option<bool>,
}


#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SetExpr {
    Select(Box<Select>),
    Values(Values),
}



#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TableFactor {
    Table {
        name: String,
        alias: Option<TableAlias>,
        /// Query planner result
        index: Option<IndexItem>,
    },
    Derived {
        subquery: Query,
        alias: TableAlias,
    },
    Series {
        alias: TableAlias,
        size: Expr,
    },
}




#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum IndexItem {
    PrimaryKey(Expr),
    NonClustered {
        name: String,
        asc: Option<bool>,
        cmp_expr: Option<(IndexOperator, Expr)>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SelectItem {
    /// An expression
    Expr { expr: Expr, label: String },
    /// `alias.*` or even `schema.table.*`
    QualifiedWildcard(String),
    /// An unqualified `*`
    Wildcard,
}

