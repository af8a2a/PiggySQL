use std::sync::Arc;

use itertools::Itertools;
use sqlparser::ast::{ObjectName, OrderByExpr};

use crate::{
    binder::{lower_case_name, split_name},
    execution::executor::ddl::drop_index::DropIndex,
    planner::{
        operator::{create_index::CreateIndexOperator, drop_index::DropIndexOperator, Operator},
        LogicalPlan,
    },
    storage::Transaction,
};

use super::{BindError, Binder};

impl<'a, T: Transaction> Binder<'a, T> {
    pub(crate) fn bind_drop_index(
        &mut self,
        names: &ObjectName,
        if_exists: &bool,
    ) -> Result<LogicalPlan, BindError> {
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
