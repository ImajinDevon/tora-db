use derive_more::Display;
use tora::{ReadEnum, ReadStruct, WriteEnum, WriteStruct};

pub mod engine;

/// An index of a row or column.
pub type Index = u32;

#[derive(Display, Debug, ReadEnum, WriteEnum)]
pub enum Id {
    #[display("`{_0}`")]
    Name(String),

    #[display("({_0})")]
    Index(Index),
}

#[derive(Display, ReadEnum, WriteEnum)]
pub enum Instruction {
    #[display("DELETE_COL @{_0}")]
    DeleteColumn(Id),

    #[display("DELETE_ROW @({_0})")]
    DeleteRow(Index),

    #[display("APPEND_ROW {_0:?}")]
    AppendRow(Vec<Data>),

    #[display("APPEND_COL `{_0}` OF `{_1}`")]
    AppendColumn(String, Type),

    #[display("FETCH @({_0}) FROM @({_1})")]
    Fetch(Index, Index),
}

#[derive(Display, Debug, PartialEq, ReadEnum, WriteEnum, Clone)]
pub enum Type {
    Int,
    Long,
    Float,
    Double,
    String,
}

#[derive(Display, Debug, PartialEq, ReadEnum, WriteEnum, Clone)]
pub enum Data {
    #[display("{_0}int")]
    Int(i32),

    #[display("{_0}long")]
    Long(i64),

    #[display("{_0}float")]
    Float(f32),

    #[display("{_0}double")]
    Double(f64),

    #[display("`{_0}`str")]
    String(String),

    #[display("NULL")]
    Null,
}

impl Data {
    pub const fn get_type(&self) -> Type {
        match self {
            Self::Int(_) => Type::Int,
            Self::Long(_) => Type::Long,
            Self::Float(_) => Type::Float,
            Self::Double(_) => Type::Double,
            Self::String(_) => Type::String,
            Self::Null => Type::String,
        }
    }
}

#[derive(Display, WriteStruct, ReadStruct)]
#[display("[`{name}`|{ty_restriction}]")]
pub struct Column {
    name: String,
    ty_restriction: Type,
}

impl Column {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub const fn ty_restriction(&self) -> &Type {
        &self.ty_restriction
    }

    pub const fn new(name: String, ty_restriction: Type) -> Self {
        Self {
            name,
            ty_restriction,
        }
    }
}
