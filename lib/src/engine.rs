use std::io;
use std::path::Path;

use derive_more::{Display, Error};
use tora::read::ToraRead;
use tora::{ReadStruct, WriteStruct};

use crate::{Column, Data, Id, Index, Instruction, Type};

/// A row of data.
pub type Row = Vec<Data>;

/// A [Result] containing a [QueryResponse] on success, and a [QueryError] on error.
pub type QueryResult = Result<QueryResponse, QueryError>;

/// A failure response from the database.
#[derive(Display, Debug, Error)]
pub enum QueryError {
    /// Attempted to query the database with an index that is out of bounds.
    #[display("Index out of bounds")]
    IndexOutOfBounds,

    /// Attempted to fetch data from the database with an index that is out of bounds.
    #[display("Data out of bounds")]
    DataOutOfBounds,

    /// Attempted to query an object that does not exist.
    #[display("Item not found")]
    NotFound,

    /// The data did not fit the restrictions of the database.
    #[display("Data does not fit restrictions")]
    DataMismatch,

    #[display("Type mismatch: {_0}, {_1}")]
    TypeMismatch(Type, Type),
}

/// A success response from the database.
#[derive(Display, Debug)]
pub enum QueryResponse {
    /// Returned from successful single operations such as
    /// `DELETE_ROW`/`DELETE_COL`/`APPEND_ROW`/`APPEND_COL`.
    #[display("Ok at index: {_0}")]
    Ok(Index),

    /// Returned from successful multiple column operations.
    #[display("Modified {} columns", _0.len())]
    ModifiedColumns(Vec<Id>),

    /// Returned from successful multiple row operations.
    #[display("Modified {} rows", _0.len())]
    ModifiedRows(Vec<Index>),

    /// A single value returned from `FETCH_VALUE`.
    #[display("Returned single value: {_0}")]
    OkSingle(Data),
}

/// A simple database.
/// 
/// All data is stored in the rows, while the columns are for type checking and data validation.
#[derive(Default, ReadStruct, WriteStruct)]
pub struct Db {
    columns: Vec<Column>,
    rows: Vec<Row>,
}

impl Db {
    /// Deletes the column which exactly matches the given name.
    ///
    /// Always returns [Ok] with some value.
    pub fn delete_column_by_name(&mut self, name: &str) -> QueryResult {
        for (i, col) in self.columns.iter().enumerate() {
            if col.name == name {
                self.columns.remove(i);
                
                for row in &mut self.rows {
                    row.remove(i);
                }
                return Ok(QueryResponse::Ok(i as Index));
            }
        }
        Err(QueryError::NotFound)
    }

    /// Deletes a column by its index.
    ///
    /// Returns an [Err] if the index is greater than or equal to the amount of columns.
    pub fn delete_column_by_index(&mut self, index: Index) -> QueryResult {
        if (index as usize) < self.columns.len() {
            self.columns.remove(index as usize);

            for row in &mut self.rows {
                row.remove(index as usize);
            }
            return Ok(QueryResponse::Ok(index));
        }
        Err(QueryError::IndexOutOfBounds)
    }

    /// Deletes a row by its index.
    pub fn delete_row_by_index(&mut self, index: Index) -> QueryResult {
        if (index as usize) < self.rows.len() {
            self.rows.remove(index as usize);
            return Ok(QueryResponse::Ok(index));
        }
        Err(QueryError::IndexOutOfBounds)
    }

    /// Creates and appends a new column with the given name and type restriction.
    /// 
    /// A `NULL` value will be appended to all rows.
    pub fn append_column(&mut self, name: String, ty_restrict: Type) -> QueryResult {
        self.append_column_default(name, ty_restrict, Data::Null)
    }

    /// Creates and appends a new column with the given name and type restriction.
    ///
    /// The provided default value will be appended to all rows.
    pub fn append_column_default(&mut self, name: String, ty_restrict: Type, default: Data) -> QueryResult {
        self.columns.push(Column::new(name, ty_restrict));

        for row in &mut self.rows {
            row.push(default.clone());
        }
        Ok(QueryResponse::Ok((self.columns.len() as Index) - 1))
    }

    /// Creates and appends a new row with the given data.
    pub fn append_row(&mut self, data: Row) -> QueryResult {
        if data.len() != self.columns.len() {
            return Err(QueryError::DataMismatch);
        }
        for (i, val) in data.iter().enumerate() {
            let restriction = self.columns[i].ty_restriction.clone();

            if restriction != val.get_type() {
                return Err(QueryError::TypeMismatch(restriction, val.get_type()));
            }
        }
        self.rows.push(data);
        Ok(QueryResponse::Ok((self.rows.len() as Index) - 1))
    }

    /// Fetches a singular value according to the given query.
    pub fn fetch_value(&mut self, data_index: Index, row_index: Index) -> QueryResult {
        if data_index as usize >= self.rows.len() {
            return Err(QueryError::DataOutOfBounds);
        }
        let row = &self.rows[row_index as usize];
        let data = &row[data_index as usize];
        Ok(QueryResponse::OkSingle(data.clone()))
    }

    /// Queries the database with the given instruction.
    pub fn query(&mut self, instruction: Instruction) -> QueryResult {
        match instruction {
            Instruction::DeleteColumn(id) => match id {
                Id::Name(name) => self.delete_column_by_name(&name),
                Id::Index(i) => self.delete_column_by_index(i),
            },
            Instruction::DeleteRow(index) => self.delete_row_by_index(index),
            Instruction::AppendColumn(name, ty) => self.append_column(name, ty),
            Instruction::AppendRow(data) => self.append_row(data),
            Instruction::Fetch(i_data, i_row) => self.fetch_value(i_data, i_row),
        }
    }

    /// Wrapper method for [tora::write_to_file].
    pub fn write_to_file<P>(&self, path: P) -> io::Result<()>
    where P: AsRef<Path> {
        tora::write_to_file(path, self)
    }
    
    /// Constructs a new Db.
    pub const fn new(columns: Vec<Column>, rows: Vec<Row>) -> Self {
        Self { columns, rows }
    }
}

/// The reason that the DB failed to load.
#[derive(Display, Debug)]
pub enum LoadDbErrorKind {
    Malformed,
    Io,
}

/// An error produced by the DB when loading itself from a byte source fails.
#[derive(Display, Debug, Error)]
#[display("{kind}: {message}")]
pub struct LoadDbError {
    message: String,
    kind: LoadDbErrorKind,
}

impl LoadDbError {
    pub fn message(&self) -> &str {
        &self.message
    }

    pub const fn kind(&self) -> &LoadDbErrorKind {
        &self.kind
    }

    const fn new(message: String, kind: LoadDbErrorKind) -> Self {
        Self { message, kind }
    }
}

impl From<io::Error> for LoadDbError {
    fn from(value: io::Error) -> Self {
        if value.kind() == io::ErrorKind::InvalidData {
            return Self::new(value.to_string(), LoadDbErrorKind::Malformed);
        }
        Self::new(value.to_string(), LoadDbErrorKind::Io)
    }
}

impl TryFrom<&[u8]> for Db {
    type Error = LoadDbError;

    fn try_from(mut value: &[u8]) -> Result<Self, Self::Error> {
        Ok(value.reads()?)
    }
}
