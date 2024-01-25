use std::{collections::HashSet, sync::Arc};

use sqlparser::ast::{ColumnDef, Ident, ObjectName, TableConstraint};

use crate::{
    plan::{
        operator::{create_table::CreateTableOperator, Operator},
        LogicalPlan,
    },
    store::{schema::Column, StorageMut, Store},
    types::DataType,
};

use super::{split_name, BindError, Binder};

impl<'a, S: Store + StorageMut> Binder<'a, S> {
    pub(crate) fn bind_create_table(
        &mut self,
        name: &ObjectName,
        columns: &[ColumnDef],
        constraints: &[TableConstraint],
        if_not_exists: bool,
    ) -> Result<LogicalPlan, BindError> {
        let name = ObjectName(
            name.0
                .iter()
                .map(|ident| Ident::new(ident.value.to_lowercase()))
                .collect(),
        );
        let (_, name) = split_name(&name)?;
        let table_name = name.to_string();
        {
            // check duplicated column names
            let mut set = HashSet::new();
            for col in columns.iter() {
                let col_name = &col.name.value;
                if !set.insert(col_name.clone()) {
                    return Err(BindError::AmbiguousColumn(col_name.to_string()));
                }
            }
        }
        let mut columns: Vec<Column> = columns
            .iter()
            .map(|col| self.bind_column(col))
            .try_collect()?;

        for constraint in constraints {
            match constraint {
                TableConstraint::Unique {
                    columns: column_names,
                    is_primary,
                    ..
                } => {
                    for column_name in column_names {
                        if let Some(column) = columns
                            .iter_mut()
                            .find(|column| column.column_name == column_name.to_string())
                        {
                            if *is_primary {
                                column.is_primary = true;
                            } else {
                                column.is_unique = true;
                            }
                        }
                    }
                }
                _ => todo!(),
            }
        }
        if columns.iter().filter(|col| col.is_primary).count() != 1 {
            return Err(BindError::InvalidTable(
                "The primary key field must exist and have at least one".to_string(),
            ));
        }
        let plan = LogicalPlan {
            operator: Operator::CreateTable(CreateTableOperator {
                table_name,
                columns,
                if_not_exists,
            }),
            childrens: vec![],
        };

        Ok(plan)
    }
    //todo
    pub fn bind_column(&mut self, column_def: &ColumnDef) -> Result<Column, BindError> {
        let column_name = column_def.name.to_string();
        let mut col = Column::new(
            column_def.name.to_string(),
            DataType::try_from(column_def.data_type.clone())?,
            false,
            false,
            false,
        );
        Ok(col)
    }
}


