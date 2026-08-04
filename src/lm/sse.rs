use anyhow::anyhow;

use crate::error::{Result as SdkResult, SdkError};

pub(crate) fn append_utf8(
    buffer: &mut String,
    incomplete_utf8: &mut Vec<u8>,
    chunk: &[u8],
) -> SdkResult<()> {
    if incomplete_utf8.is_empty() {
        append_bytes(buffer, incomplete_utf8, chunk)
    } else {
        let mut combined = std::mem::take(incomplete_utf8);
        combined.extend_from_slice(chunk);
        append_bytes(buffer, incomplete_utf8, &combined)
    }
}

fn append_bytes(buffer: &mut String, incomplete_utf8: &mut Vec<u8>, bytes: &[u8]) -> SdkResult<()> {
    match std::str::from_utf8(bytes) {
        Ok(text) => buffer.push_str(text),
        Err(err) if err.error_len().is_none() => {
            let valid_up_to = err.valid_up_to();
            let valid = std::str::from_utf8(&bytes[..valid_up_to])
                .expect("from_utf8 reported this prefix as valid");
            buffer.push_str(valid);
            incomplete_utf8.extend_from_slice(&bytes[valid_up_to..]);
        }
        Err(err) => {
            return Err(SdkError::Other(anyhow!(
                "invalid UTF-8 in SSE stream: {err}"
            )));
        }
    }

    Ok(())
}
