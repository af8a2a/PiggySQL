use crate::result::{Error,Result};

pub trait Transaction:Sync + Send + 'static{
    async fn begin(&mut self, autocommit: bool) -> Result<bool> {
        if autocommit {
            return Ok(false);
        }

        Err(Error::StorageMsg(
            "[Storage] Transaction::begin is not supported".to_owned(),
        ))
    }

    async fn rollback(&mut self) -> Result<()> {
        Ok(())
    }

    async fn commit(&mut self) -> Result<()> {
        Ok(())
    }
}
