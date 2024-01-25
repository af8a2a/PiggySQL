



pub enum DataValue {
    Null,
    Boolean(Option<bool>),
    Float(Option<f32>),
    Interger(Option<i8>),
    Utf8(Option<String>),
}

pub enum DataType {
    Null,
    Boolean,
    Float,
    Interger,
    Utf8,

}