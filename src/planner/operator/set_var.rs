use std::fmt;

#[derive(Debug, PartialEq, Clone)]
pub struct SetVarOperator {
    pub variable: String,
    pub value: String,
}
impl fmt::Display for SetVarOperator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Set {} = {}", self.variable, self.value)?;
        Ok(())
    }
}
