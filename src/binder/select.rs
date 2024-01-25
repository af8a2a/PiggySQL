// use std::borrow::Borrow;

// use futures::FutureExt;
// use itertools::Itertools;
// use sqlparser::ast::{Ident, OrderByExpr, Query, Select, SelectItem, SetExpr, TableFactor, TableWithJoins};

// use crate::{
//     expression::ScalarExpression, plan::{
//         operator::{join::JoinType, scan::ScanOperator, Operator},
//         LogicalPlan,
//     }, store::{StorageMut, Store}
// };

// use super::{BindError, Binder, TableName};

// impl<'a, S: Store + StorageMut> Binder<'a, S> {
//     pub(crate) fn bind_query(&mut self, query: &Query) -> Result<LogicalPlan, BindError> {
//         let mut plan = match query.body.borrow() {
//             SetExpr::Select(select) => self.bind_select(select, &query.order_by),
//             SetExpr::Query(query) => self.bind_query(query),
//             _ => unimplemented!(),
//         }?;
//         todo!()
//     }
//     fn bind_select(
//         &mut self,
//         select: &Select,
//         orderby: &[OrderByExpr],
//     ) -> Result<LogicalPlan, BindError> {
//         let mut plan = self.bind_table_ref(&select.from)?;
//         let mut select_list = self.normalize_select_item(&select.projection)?;

//         todo!()
//     }

//     pub(crate) fn bind_table_ref(
//         &mut self,
//         from: &[TableWithJoins],
//     ) -> Result<LogicalPlan, BindError> {
//         assert!(from.len() < 2, "not support yet.");
//         if from.is_empty() {
//             return Ok(LogicalPlan {
//                 operator: Operator::Dummy,
//                 childrens: vec![],
//             });
//         }
//         let TableWithJoins { relation, joins } = &from[0];
//         let (left_name, mut plan) = self.bind_single_table_ref(relation, None)?;

//         if !joins.is_empty() {
//             let left_name = Self::unpack_name(left_name, true);

//             for join in joins {
//                 plan = self.bind_join(left_name.clone(), plan, join)?;
//             }
//         }
//         Ok(plan)
//     }

//     async fn bind_single_table_ref(
//         &mut self,
//         table: &TableFactor,
//         joint_type: Option<JoinType>,
//     ) -> Result<(Option<TableName>, LogicalPlan), BindError> {
//         let plan_with_name = match table {
//             TableFactor::Table { name, alias, .. } => {
//                 let obj_name = name
//                     .0
//                     .iter()
//                     .map(|ident| Ident::new(ident.value.to_lowercase()))
//                     .collect_vec();

//                 let (_database, _schema, table): (&str, &str, &str) = match obj_name.as_slice() {
//                     //todo
//                     [database, schema, table] => (&database.value, &schema.value, &table.value),
//                     _ => return Err(BindError::InvalidTableName(obj_name)),
//                 };
//                 let table_catalog = match self.context.storage.fetch_schema(table).await.expect("")
//                 {
//                     Some(schema) => schema,
//                     None => return Err(BindError::InvalidTable(table.to_string())),
//                 };

//                 self.context
//                     .bind_table
//                     .insert(table.to_string(), (table_catalog.clone(), joint_type));

//                 if let Some(alias) = alias {
//                     self.context
//                         .table_aliases
//                         .insert(alias.to_string(), table.to_string());
//                 }
//                 let plan = ScanOperator::build(table.to_string(), &table_catalog);

//                 (Some(table.to_string()), plan)
//             }
//             TableFactor::Derived {
//                 subquery,
//                 alias,
//                 ..
//             } => {
//                 let plan = self.bind_query(subquery)?;

//             },
//             _ => unimplemented!(),
//         };
//         Ok(plan_with_name)
//     }


//         /// Normalize select item.
//     ///
//     /// - Qualified name, e.g. `SELECT t.a FROM t`
//     /// - Qualified name with wildcard, e.g. `SELECT t.* FROM t,t1`
//     /// - Scalar expression or aggregate expression, e.g. `SELECT COUNT(*) + 1 AS count FROM t`
//     ///  
//     fn normalize_select_item(
//         &mut self,
//         items: &[SelectItem],
//     ) -> Result<Vec<ScalarExpression>, BindError> {
//         let mut select_items = vec![];

//         for item in items.iter().enumerate() {
//             match item.1 {
//                 SelectItem::UnnamedExpr(expr) => select_items.push(self.bind_expr(expr)?),
//                 SelectItem::ExprWithAlias { expr, alias } => {
//                     let expr = self.bind_expr(expr)?;
//                     let alias_name = alias.to_string();

//                     self.context.add_alias(alias_name.clone(), expr.clone())?;

//                     select_items.push(ScalarExpression::Alias {
//                         expr: Box::new(expr),
//                         alias: alias_name,
//                     });
//                 }
//                 SelectItem::Wildcard(_) => {
//                     select_items.extend_from_slice(self.bind_all_column_refs()?.as_slice());
//                 }

//                 _ => todo!("bind select list"),
//             };
//         }

//         Ok(select_items)
//     }
// }
