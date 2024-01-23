use {
    super::expr::Expr,
    serde::{Deserialize, Serialize},
};
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Aggregate {
    Count(CountArgExpr),
    Sum(Expr),
    Max(Expr),
    Min(Expr),
    Avg(Expr),
    Variance(Expr),
    Stdev(Expr),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CountArgExpr {
    //count (expr)
    Expr(Expr),
    //count(*)
    Wildcard,
}
