use crate::execution::executor::{Source, Executor};

use crate::planner::operator::drop_table::DropTableOperator;
use crate::storage::Transaction;
use crate::types::tuple_builder::TupleBuilder;



pub struct DropTable {
    op: DropTableOperator,
}

impl From<DropTableOperator> for DropTable {
    fn from(op: DropTableOperator) -> Self {
        DropTable { op }
    }
}

impl<T: Transaction> Executor<T> for DropTable {
    fn execute(self, transaction: &mut T) -> Source {
        let DropTableOperator {
            table_name,
            if_exists,
        } = self.op;

        transaction.drop_table(&table_name.clone(), if_exists)?;
        let tuple_builder = TupleBuilder::new_result();
        let tuple =
            tuple_builder.push_result("DROP TABLE SUCCESS", format!("{}", table_name).as_str())?;

        Ok(vec![tuple])
    }
}
