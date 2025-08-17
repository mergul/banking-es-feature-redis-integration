use byteorder::{BigEndian, WriteBytesExt};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use std::io::{Cursor, Write};
use uuid::Uuid;

/// Fixed PostgreSQL COPY binary format writer
pub struct PgCopyBinaryWriter {
    buffer: Vec<u8>,
    tuple_count: usize,
}

impl PgCopyBinaryWriter {
    pub fn new() -> Result<Self, std::io::Error> {
        let mut buffer = Vec::new();

        // CRITICAL FIX 1: Write header with proper byte order
        buffer.write_all(b"PGCOPY\n\xff\r\n\0")?; // 11-byte signature - CORRECT
        buffer.write_u32::<BigEndian>(0)?; // Flags field - FIXED: use BigEndian
        buffer.write_u32::<BigEndian>(0)?; // Header extension length - FIXED: use BigEndian

        Ok(Self {
            buffer,
            tuple_count: 0,
        })
    }

    /// Write tuple header with field count
    pub fn write_row(&mut self, column_count: i16) -> Result<(), std::io::Error> {
        if column_count < 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("Column count must be non-negative, got: {}", column_count),
            ));
        }
        // CORRECT: PostgreSQL expects i16 for field count
        self.buffer.write_i16::<BigEndian>(column_count)?;
        self.tuple_count += 1;
        Ok(())
    }

    /// Write UUID field (16 bytes)
    pub fn write_uuid(&mut self, uuid: &Uuid) -> Result<(), std::io::Error> {
        self.buffer.write_i32::<BigEndian>(16)?; // Length: 16 bytes
        self.buffer.write_all(uuid.as_bytes())?; // UUID bytes in standard format
        Ok(())
    }

    /// Write text field with UTF-8 encoding
    pub fn write_text(&mut self, text: &str) -> Result<(), std::io::Error> {
        let bytes = text.as_bytes();
        if bytes.len() > i32::MAX as usize {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Text field too large",
            ));
        }
        self.buffer.write_i32::<BigEndian>(bytes.len() as i32)?;
        self.buffer.write_all(bytes)?;
        Ok(())
    }

    /// Write bytea field
    pub fn write_bytea(&mut self, data: &[u8]) -> Result<(), std::io::Error> {
        if data.len() > i32::MAX as usize {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Bytea field too large",
            ));
        }
        self.buffer.write_i32::<BigEndian>(data.len() as i32)?;
        self.buffer.write_all(data)?;
        Ok(())
    }

    /// CRITICAL FIX 2: Corrected timestamp calculation
    pub fn write_timestamp(&mut self, timestamp: &DateTime<Utc>) -> Result<(), std::io::Error> {
        // PostgreSQL timestamp: microseconds since 2000-01-01 00:00:00 UTC
        // Unix timestamp: seconds since 1970-01-01 00:00:00 UTC
        // Difference: 946684800 seconds (30 years)

        const PG_EPOCH_OFFSET_SECONDS: i64 = 946684800;

        // Convert to microseconds since PostgreSQL epoch
        let unix_seconds = timestamp.timestamp();
        let unix_microseconds = timestamp.timestamp_subsec_micros() as i64;
        let total_unix_microseconds = unix_seconds * 1_000_000 + unix_microseconds;
        let pg_microseconds = total_unix_microseconds - (PG_EPOCH_OFFSET_SECONDS * 1_000_000);

        self.buffer.write_i32::<BigEndian>(8)?; // Length: 8 bytes
        self.buffer.write_i64::<BigEndian>(pg_microseconds)?; // FIXED: use BigEndian
        Ok(())
    }

    /// CRITICAL FIX 3: Fixed bigint writing
    pub fn write_bigint(&mut self, value: i64) -> Result<(), std::io::Error> {
        self.buffer.write_i32::<BigEndian>(8)?; // Length: 8 bytes
        self.buffer.write_i64::<BigEndian>(value)?; // FIXED: use BigEndian, not to_be_bytes
        Ok(())
    }

    /// Write boolean field
    pub fn write_bool(&mut self, value: bool) -> Result<(), std::io::Error> {
        self.buffer.write_i32::<BigEndian>(1)?; // Length: 1 byte
        self.buffer.write_u8(if value { 1 } else { 0 })?;
        Ok(())
    }

    /// CRITICAL FIX 4: Added decimal support (was missing)
    pub fn write_decimal(&mut self, decimal: &Decimal, field_name: &str) -> Result<(), std::io::Error> {
    let pos_before = self.buffer.len();
    
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
            std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid digit group in decimal")
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
    self.buffer.write_u16::<BigEndian>(ndigits)?;  // Number of digit groups
    self.buffer.write_i16::<BigEndian>(weight)?;   // Weight
    self.buffer.write_u16::<BigEndian>(sign)?;     // Sign
    self.buffer.write_u16::<BigEndian>(dscale)?;   // Display scale
    
    // Write digit groups
    for digit_word in digit_words {
        self.buffer.write_u16::<BigEndian>(digit_word)?;
    }
    
    // debug_mode is only available on Diagnostic writer; keep silent here
    
    Ok(())
}

    /// Write NULL value
    pub fn write_null(&mut self) -> Result<(), std::io::Error> {
        self.buffer.write_i32::<BigEndian>(-1)?; // -1 indicates NULL
        Ok(())
    }

    /// CRITICAL FIX 5: Improved JSONB writing
    pub fn write_jsonb(&mut self, json_value: &serde_json::Value) -> Result<(), std::io::Error> {
        match json_value {
            serde_json::Value::Null => {
                self.write_null()?;
            }
            _ => {
                let json_bytes = serde_json::to_vec(json_value)?;
                
                // For JSONB, the binary format is:
                // 1 byte: version (currently 1)
                // N bytes: JSON data
                let data_len = 1 + json_bytes.len();
                self.buffer.write_i32::<BigEndian>(data_len as i32)?;
                self.buffer.write_u8(1)?; // JSONB version
                self.buffer.write_all(&json_bytes)?;
            }
        }
        Ok(())
    }

    /// CRITICAL FIX 6: Alternative JSONB as bytea (more efficient)
    pub fn write_jsonb_binary(
        &mut self,
        json_value: &serde_json::Value,
    ) -> Result<(), std::io::Error> {
        match json_value {
            serde_json::Value::Null => {
                self.write_null()?;
            }
            _ => {
                let json_bytes = serde_json::to_vec(json_value)?;

                // For JSONB, the binary format is:
                // 1 byte: version (currently 1)
                // N bytes: JSON data
                let data_len = 1 + json_bytes.len();
                self.buffer.write_i32::<BigEndian>(data_len as i32)?;
                self.buffer.write_u8(1)?; // JSONB version
                self.buffer.write_all(&json_bytes)?;
            }
        }
        Ok(())
    }

    /// Write trailer and finish
    pub fn finish(mut self) -> Result<Vec<u8>, std::io::Error> {
        // CORRECT: Write trailer as i16 value of -1
        self.buffer.write_i16::<BigEndian>(-1)?;
        Ok(self.buffer)
    }

    /// Get number of tuples written
    pub fn get_tuple_count(&self) -> usize {
        self.tuple_count
    }

    /// Get current buffer size (for debugging)
    pub fn get_buffer_size(&self) -> usize {
        self.buffer.len()
    }
}

// DEBUGGING HELPER: Dump binary data for inspection
pub fn debug_binary_data(data: &[u8], max_bytes: usize) -> String {
    let bytes_to_show = std::cmp::min(data.len(), max_bytes);
    let hex_dump: Vec<String> = data[..bytes_to_show]
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect();

    format!(
        "Binary data: {} bytes total, first {} bytes: {}{}",
        data.len(),
        bytes_to_show,
        hex_dump.join(" "),
        if data.len() > max_bytes { "..." } else { "" }
    )
}

/// Helper trait for converting data to PGCOPY binary format
pub trait ToPgCopyBinary {
    fn to_pgcopy_binary(&self) -> Result<Vec<u8>, std::io::Error>;
}

/// Diagnostic PostgreSQL COPY binary writer with extensive logging
pub struct DiagnosticPgCopyBinaryWriter {
    buffer: Vec<u8>,
    tuple_count: usize,
    debug_mode: bool,
}

impl DiagnosticPgCopyBinaryWriter {
    pub fn new(debug_mode: bool) -> Result<Self, std::io::Error> {
        let mut buffer = Vec::new();
        
        if debug_mode {
            tracing::debug!("🔍 Starting PostgreSQL COPY BINARY header creation");
        }

        // Write PostgreSQL COPY BINARY header
        let signature = b"PGCOPY\n\xff\r\n\0";
        buffer.write_all(signature)?;
        if debug_mode {
            tracing::debug!("✅ Wrote 11-byte signature: {:?}", signature);
        }

        // Flags field (32-bit, big endian, value = 0)
        buffer.write_u32::<BigEndian>(0)?;
        if debug_mode {
            tracing::debug!("✅ Wrote flags: 0 (4 bytes)");
        }

        // Header extension length (32-bit, big endian, value = 0)
        buffer.write_u32::<BigEndian>(0)?;
        if debug_mode {
            tracing::debug!("✅ Wrote header extension length: 0 (4 bytes)");
            tracing::debug!("🔍 Total header size: {} bytes", buffer.len());
        }

        Ok(Self { 
            buffer, 
            tuple_count: 0,
            debug_mode,
        })
    }

    pub fn write_row(&mut self, column_count: i16) -> Result<(), std::io::Error> {
        
        if column_count < 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("Column count must be non-negative, got: {}", column_count)
            ));
        }
        
        let pos_before = self.buffer.len();
        self.buffer.write_i16::<BigEndian>(column_count)?;
        self.tuple_count += 1;
        
        if self.debug_mode {
            tracing::debug!("✅ Wrote tuple header: {} columns (2 bytes) at position {}", column_count, pos_before);
        }
        
        Ok(())
    }

    pub fn write_uuid(&mut self, uuid: &Uuid, field_name: &str) -> Result<(), std::io::Error> {
        let pos_before = self.buffer.len();
        
        // Length field (4 bytes)
        self.buffer.write_i32::<BigEndian>(16)?;
        
        // UUID data (16 bytes)
        let uuid_bytes = uuid.as_bytes();
        self.buffer.write_all(uuid_bytes)?;
        
        if self.debug_mode {
            tracing::debug!(
                "✅ Wrote UUID field '{}': {} (20 bytes total: 4-byte length + 16-byte data) at position {}",
                field_name, uuid, pos_before
            );
        }
        
        Ok(())
    }

    pub fn write_text(&mut self, text: &str, field_name: &str) -> Result<(), std::io::Error> {
        let pos_before = self.buffer.len();
        let text_bytes = text.as_bytes();
        
        if text_bytes.len() > i32::MAX as usize {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("Text field '{}' too large: {} bytes", field_name, text_bytes.len())
            ));
        }
        
        let length = text_bytes.len() as i32;
        
        // Length field (4 bytes)
        self.buffer.write_i32::<BigEndian>(length)?;
        
        // Text data (variable bytes)
        self.buffer.write_all(text_bytes)?;
        
        if self.debug_mode {
            tracing::debug!(
                "✅ Wrote TEXT field '{}': '{}' ({} bytes total: 4-byte length + {}-byte data) at position {}",
                field_name, 
                if text.len() > 50 { format!("{}...", &text[..50]) } else { text.to_string() },
                4 + length,
                length,
                pos_before
            );
        }
        
        Ok(())
    }

    pub fn write_decimal(&mut self, decimal: &Decimal, field_name: &str) -> Result<(), std::io::Error> {
    let pos_before = self.buffer.len();
    
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
            std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid digit group in decimal")
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
    self.buffer.write_u16::<BigEndian>(ndigits)?;  // Number of digit groups
    self.buffer.write_i16::<BigEndian>(weight)?;   // Weight
    self.buffer.write_u16::<BigEndian>(sign)?;     // Sign
    self.buffer.write_u16::<BigEndian>(dscale)?;   // Display scale
    
    // Write digit groups
    for digit_word in digit_words {
        self.buffer.write_u16::<BigEndian>(digit_word)?;
    }
    
    if self.debug_mode {
        tracing::debug!(
            "✅ Wrote NUMERIC field '{}': '{}' -> ndigits:{}, weight:{}, sign:{}, dscale:{} ({} bytes total) at position {}",
            field_name, decimal_str, ndigits, weight, sign, dscale, 4 + data_size, pos_before
        );
    }
    
    Ok(())
}

    pub fn write_bool(&mut self, value: bool, field_name: &str) -> Result<(), std::io::Error> {
        let pos_before = self.buffer.len();
        
        // Length field (4 bytes, value = 1)
        self.buffer.write_i32::<BigEndian>(1)?;
        
        // Boolean data (1 byte: 1 for true, 0 for false)
        self.buffer.write_u8(if value { 1 } else { 0 })?;
        
        if self.debug_mode {
            tracing::debug!(
                "✅ Wrote BOOLEAN field '{}': {} (5 bytes total: 4-byte length + 1-byte data) at position {}",
                field_name, value, pos_before
            );
        }
        
        Ok(())
    }

    pub fn write_timestamp(&mut self, timestamp: &DateTime<Utc>, field_name: &str) -> Result<(), std::io::Error> {
        let pos_before = self.buffer.len();
        
        // PostgreSQL epoch: 2000-01-01 00:00:00 UTC
        // Unix epoch: 1970-01-01 00:00:00 UTC
        // Difference: 946684800 seconds
        const PG_EPOCH_OFFSET_SECONDS: i64 = 946684800;
        
        let unix_timestamp_secs = timestamp.timestamp();
        let unix_timestamp_micros = timestamp.timestamp_subsec_micros() as i64;
        let total_unix_micros = unix_timestamp_secs * 1_000_000 + unix_timestamp_micros;
        let pg_micros = total_unix_micros - (PG_EPOCH_OFFSET_SECONDS * 1_000_000);
        
        // Length field (4 bytes, value = 8)
        self.buffer.write_i32::<BigEndian>(8)?;
        
        // Timestamp data (8 bytes, microseconds since PostgreSQL epoch)
        self.buffer.write_i64::<BigEndian>(pg_micros)?;
        
        if self.debug_mode {
            tracing::debug!(
                "✅ Wrote TIMESTAMP field '{}': {} -> PG micros: {} (12 bytes total: 4-byte length + 8-byte data) at position {}",
                field_name, timestamp.format("%Y-%m-%d %H:%M:%S%.6f UTC"), pg_micros, pos_before
            );
        }
        
        Ok(())
    }

    pub fn write_null(&mut self, field_name: &str) -> Result<(), std::io::Error> {
        let pos_before = self.buffer.len();
        
        // NULL is represented by length = -1
        self.buffer.write_i32::<BigEndian>(-1)?;
        
        if self.debug_mode {
            tracing::debug!(
                "✅ Wrote NULL field '{}' (4 bytes: length = -1) at position {}",
                field_name, pos_before
            );
        }
        
        Ok(())
    }

    pub fn finish(mut self) -> Result<Vec<u8>, std::io::Error> {
        let pos_before = self.buffer.len();
        
        // Write trailer: -1 as i16
        self.buffer.write_i16::<BigEndian>(-1)?;
        
        if self.debug_mode {
            tracing::debug!("✅ Wrote trailer: -1 (2 bytes) at position {}", pos_before);
            tracing::debug!("🎯 Final binary data: {} bytes total for {} tuples", self.buffer.len(), self.tuple_count);
            
            // Log first 100 bytes for inspection
            let preview_len = std::cmp::min(100, self.buffer.len());
            let hex_preview: String = self.buffer[..preview_len]
                .iter()
                .map(|b| format!("{:02x}", b))
                .collect::<Vec<_>>()
                .join(" ");
            tracing::debug!("🔍 Binary data preview (first {} bytes): {}", preview_len, hex_preview);
        }
        
        Ok(self.buffer)
    }

    pub fn get_tuple_count(&self) -> usize {
        self.tuple_count
    }

    pub fn get_buffer_size(&self) -> usize {
        self.buffer.len()
    }
}
