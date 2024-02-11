use std::sync::Arc;

use sqlparser::ast::ObjectName;

use crate::errors::*;
use crate::{
    planner::{
        operator::{drop_index::DropIndexOperator, Operator},
        LogicalPlan,
    },
    storage::Transaction,
};

use super::Binder;

impl<'a, T: Transaction> Binder<'a, T> {
    pub(crate) fn bind_drop_index(
        &mut self,
        names: &ObjectName,
        if_exists: &bool,
    ) -> Result<LogicalPlan> {
        let object_name = &names.0;
        let table_name = object_name[0].value.to_owned().to_lowercase();
        let index_name = object_name[1].value.to_owned().to_lowercase();

        let table_name = Arc::new(table_name);
        let index_name = Arc::new(index_name);

        //now only support unique index
        let plan = LogicalPlan {
            operator: Operator::DropIndex(DropIndexOperator {
                table_name,
                index_name,
                if_exists: *if_exists,
            }),
            childrens: vec![],
        };
        Ok(plan)
    }
}
