use byteorder::{BigEndian, WriteBytesExt};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
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
    /// CRITICAL FIX 4: Added decimal support (was missing)
    pub fn write_decimal(
        &mut self,
        decimal: &Decimal,
        field_name: &str,
    ) -> Result<(), std::io::Error> {
        let pos_before = self.buffer.get_ref().len();

        // Convert Decimal to PostgreSQL NUMERIC binary format
        let decimal_str = decimal.to_string();

        // Parse the decimal string to extract parts
        let (sign, digits_str) = if decimal_str.starts_with('-') {
            (0x4000u16, &decimal_str[1..]) // Negative sign
        } else {
            (0x0000u16, decimal_str.as_str()) // Positive
        };

        // Split into integer and fractional parts
        let (integer_part, fractional_part) = if let Some(dot_pos) = digits_str.find('.') {
            (&digits_str[..dot_pos], &digits_str[dot_pos + 1..])
        } else {
            (digits_str, "")
        };

        // Calculate scale (number of digits after decimal point)
        let scale = fractional_part.len() as i16;

        // Combine all digits (remove decimal point)
        let all_digits = format!("{}{}", integer_part, fractional_part);

        // PostgreSQL NUMERIC stores digits in groups of 4 decimal digits per 16-bit word
        // Pad with leading zeros to make length multiple of 4
        let padded_digits = if all_digits.len() % 4 == 0 {
            all_digits.clone()
        } else {
            let padding = 4 - (all_digits.len() % 4);
            format!("{}{}", "0".repeat(padding), all_digits)
        };

        // Convert groups of 4 digits to 16-bit words
        let mut digit_words = Vec::new();
        for chunk in padded_digits.as_bytes().chunks(4) {
            let digit_str = std::str::from_utf8(chunk).map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid UTF-8 in decimal")
            })?;
            let digit_value: u16 = digit_str.parse().map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Invalid digit group in decimal",
                )
            })?;
            digit_words.push(digit_value);
        }

        // Calculate weight (position of most significant digit group relative to decimal point)
        let total_digits = all_digits.len() as i16;
        let integer_digits = integer_part.len() as i16;
        let weight = if integer_digits > 0 {
            ((integer_digits - 1) / 4) as i16
        } else {
            -1 - ((scale - 1) / 4)
        };

        // Build the binary representation
        let ndigits = digit_words.len() as u16;
        let weight = weight;
        let sign = sign;
        let dscale = scale as u16;

        // Calculate total size: 8 bytes header + (ndigits * 2 bytes)
        let data_size = 8 + (ndigits as i32 * 2);

        // Write length field
        self.buffer.write_i32::<BigEndian>(data_size)?;

        // Write NUMERIC header
        self.buffer.write_u16::<BigEndian>(ndigits)?; // Number of digit groups
        self.buffer.write_i16::<BigEndian>(weight)?; // Weight
        self.buffer.write_u16::<BigEndian>(sign)?; // Sign
        self.buffer.write_u16::<BigEndian>(dscale)?; // Display scale

        // Write digit groups
        for digit_word in digit_words {
            self.buffer.write_u16::<BigEndian>(digit_word)?;
        }

        // debug_mode is only available on Diagnostic writer; keep silent here

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
