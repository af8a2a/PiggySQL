use {
    super::TranslateError,
    crate::{ast::types::*, result::Result},
    sqlparser::ast::DataType as SqlDataType,
};


impl DataType{
    pub fn from(sql_data_type: &SqlDataType)-> Result<DataType>{
        match sql_data_type {
            SqlDataType::Boolean => Ok(DataType::Boolean),
            SqlDataType::Int(None) | SqlDataType::Integer(None) | SqlDataType::Int64 => {
                Ok(DataType::Integer)
            }
    
            SqlDataType::Float(None) | SqlDataType::Float(Some(64)) => Ok(DataType::Float),
    
            SqlDataType::Text => Ok(DataType::String),
            SqlDataType::Bytea => Ok(DataType::String),
            //not support yet
            //todo
            // SqlDataType::Decimal(SqlExactNumberInfo::None) => Ok(DataType::Decimal),
            _ => Err(TranslateError::UnsupportedDataType(sql_data_type.to_string()).into()),
        }
    
    }
}
