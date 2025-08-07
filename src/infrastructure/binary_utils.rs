use byteorder::{BigEndian, WriteBytesExt};
use chrono::{DateTime, Utc};
use std::io::{Cursor, Write};
use uuid::Uuid;

/// Unified PostgreSQL COPY binary format utilities
pub struct PgCopyBinaryWriter {
    buffer: Cursor<Vec<u8>>,
}

impl PgCopyBinaryWriter {
    pub fn new() -> Self {
        let mut buffer = Cursor::new(Vec::new());

        // Write PGCOPY header
        buffer.write_all(b"PGCOPY\n\xff\r\n\0").unwrap(); // Signature
        buffer.write_all(&[0, 0, 0, 0]).unwrap(); // Flags
        buffer.write_all(&[0, 0, 0, 0]).unwrap(); // Header extension length

        Self { buffer }
    }

    /// Write a row with specified number of columns
    pub fn write_row(&mut self, column_count: i16) -> Result<(), std::io::Error> {
        self.buffer.write_i16::<BigEndian>(column_count)?;
        Ok(())
    }

    /// Write UUID field
    pub fn write_uuid(&mut self, uuid: &Uuid) -> Result<(), std::io::Error> {
        self.buffer.write_i32::<BigEndian>(16)?; // UUID length
        self.buffer.write_all(uuid.as_bytes())?;
        Ok(())
    }

    /// Write text field
    pub fn write_text(&mut self, text: &str) -> Result<(), std::io::Error> {
        let bytes = text.as_bytes();
        self.buffer.write_i32::<BigEndian>(bytes.len() as i32)?;
        self.buffer.write_all(bytes)?;
        Ok(())
    }

    /// Write bytea field
    pub fn write_bytea(&mut self, data: &[u8]) -> Result<(), std::io::Error> {
        self.buffer.write_i32::<BigEndian>(data.len() as i32)?;
        self.buffer.write_all(data)?;
        Ok(())
    }

    /// Write JSONB data in PostgreSQL binary format for COPY
    pub fn write_jsonb(&mut self, json_value: &serde_json::Value) -> Result<(), std::io::Error> {
        match json_value {
            serde_json::Value::Null => {
                // Write NULL value (-1 length)
                self.buffer.write_i32::<BigEndian>(-1)?;
            }
            _ => {
                // Convert JSON to string
                let json_str = json_value.to_string();
                let json_bytes = json_str.as_bytes();

                // Write length (not including null terminator)
                self.buffer
                    .write_i32::<BigEndian>(json_bytes.len() as i32)?;

                // Write the JSON string bytes (UTF-8 encoded)
                self.buffer.write_all(json_bytes)?;

                // Note: No null terminator needed for binary format text fields
            }
        }
        Ok(())
    }
    /// Alternative implementation using PostgreSQL's JSONB binary format
    /// This is more complex but potentially more efficient
    pub fn write_jsonb_binary(
        &mut self,
        json_value: &serde_json::Value,
    ) -> Result<(), std::io::Error> {
        match json_value {
            serde_json::Value::Null => {
                // Write NULL value (-1 length)
                self.buffer.write_i32::<BigEndian>(-1)?;
            }
            _ => {
                // For binary COPY, we can still use text representation
                // PostgreSQL will handle the JSONB conversion
                let json_str = json_value.to_string();

                // Ensure the string is valid UTF-8 (it should be from serde_json)
                if !json_str.is_ascii() {
                    // Validate UTF-8 encoding
                    std::str::from_utf8(json_str.as_bytes())
                        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
                }

                let json_bytes = json_str.as_bytes();

                // Write length
                self.buffer
                    .write_i32::<BigEndian>(json_bytes.len() as i32)?;

                // Write the JSON string bytes
                self.buffer.write_all(json_bytes)?;
            }
        }
        Ok(())
    }

    /// Write timestamp field (PostgreSQL format)
    pub fn write_timestamp(&mut self, timestamp: &DateTime<Utc>) -> Result<(), std::io::Error> {
        let postgres_epoch = chrono::NaiveDate::from_ymd_opt(2000, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let micros_since_postgres_epoch = timestamp
            .signed_duration_since(chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(
                postgres_epoch,
                chrono::Utc,
            ))
            .num_microseconds()
            .unwrap_or(0);
        self.buffer.write_i32::<BigEndian>(8)?; // 8 bytes for timestamp
        self.buffer
            .write_all(&micros_since_postgres_epoch.to_be_bytes())?;
        Ok(())
    }

    /// Write bigint field
    pub fn write_bigint(&mut self, value: i64) -> Result<(), std::io::Error> {
        self.buffer.write_i32::<BigEndian>(8)?; // 8 bytes for bigint
        self.buffer.write_all(&value.to_be_bytes())?;
        Ok(())
    }

    /// Finish the COPY operation
    pub fn finish(mut self) -> Result<Vec<u8>, std::io::Error> {
        // Write PGCOPY trailer
        self.buffer.write_i16::<BigEndian>(-1)?;
        Ok(self.buffer.into_inner())
    }
}

/// Helper trait for converting data to PGCOPY binary format
pub trait ToPgCopyBinary {
    fn to_pgcopy_binary(&self) -> Result<Vec<u8>, std::io::Error>;
}
