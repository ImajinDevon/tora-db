# tora-db

A simple database written with the Tora crate.

## Example Usage

```rust
use tora_db::engine::Db;
use tora_db::{Data, Type};

fn main() -> Result<(), String> {
    let mut db = Db::default();

    let _ = db.append_column("Name".to_string(), Type::String);
    let _ = db.append_row(vec![Data::String("John".to_string())]);

    tora::write_to_file("test.tdb", &db).map_err(|e| e.to_string())?;

    // ... //
    
    let mut db: Db = tora::read_from_file("test.tdb").map_err(|e| e.to_string())?;
    println!("{}", db.fetch_value(0, 0).map_err(|e| e.to_string())?);
    Ok(())
}
```