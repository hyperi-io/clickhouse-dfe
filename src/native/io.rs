//! Extension traits for reading/writing `ClickHouse` native wire protocol primitives.
//!
//! Provides `VarUInt` and length-prefixed string encoding used by the native TCP protocol.

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::error::{Error, Result};

/// Hard cap on length-prefixed string and byte-array fields, matching the
/// server's default `max_string_size`.
pub(crate) const MAX_STRING_SIZE: u64 = 1 << 30;

/// A u64 needs at most 10 seven-bit groups (10 * 7 = 70 >= 64), which is
/// also where the server's `readVarUInt` stops.
pub(crate) const VAR_UINT_MAX_BYTES: usize = 10;

/// Scratch buffer wide enough for any encoded `VarUInt`.
type VarUintBuf = [u8; VAR_UINT_MAX_BYTES];

/// Encode `value` into `buf`, returning the number of bytes written.
fn encode_var_uint(mut value: u64, buf: &mut VarUintBuf) -> usize {
    let mut pos = 0;
    loop {
        #[allow(clippy::cast_possible_truncation)]
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;
        if value > 0 {
            byte |= 0x80;
        }
        buf[pos] = byte;
        pos += 1;
        if value == 0 {
            return pos;
        }
    }
}

/// Incremental `VarUInt` decoder, fed one byte at a time.
///
/// The state is shared rather than the loop because the readers below take
/// their bytes from an `AsyncRead`, a `bytes::Buf` and a plain slice.
#[derive(Default)]
struct VarUintDecoder {
    out: u64,
    index: usize,
}

impl VarUintDecoder {
    /// Absorb one byte. `Ok(Some(v))` when the value is complete,
    /// `Ok(None)` when more bytes are needed.
    fn push(&mut self, octet: u8) -> Result<Option<u64>> {
        self.out |= u64::from(octet & 0x7F) << (7 * self.index);
        self.index += 1;
        if (octet & 0x80) == 0 {
            return Ok(Some(self.out));
        }
        // Stopping short of the ceiling would truncate values >= 2^63 and
        // leave a legitimate final byte unconsumed, misaligning every later
        // read.
        if self.index == VAR_UINT_MAX_BYTES {
            return Err(Self::overflow());
        }
        Ok(None)
    }

    fn overflow() -> Error {
        Error::BadResponse("native protocol: varint continues past 10 bytes".into())
    }
}

/// Decode a `VarUInt` from the front of `bytes`, returning the value and the
/// byte count it consumed.
///
/// The sole slice decoder, so the 10-byte ceiling cannot drift from the two
/// stream readers.
///
/// # Errors
///
/// [`Error::NotEnoughData`] if `bytes` ends mid-value, [`Error::BadResponse`]
/// if the value runs past the ceiling.
pub(crate) fn get_var_uint(bytes: &[u8]) -> Result<(u64, usize)> {
    let mut decoder = VarUintDecoder::default();
    for (i, &octet) in bytes.iter().enumerate() {
        if let Some(value) = decoder.push(octet)? {
            return Ok((value, i + 1));
        }
    }
    Err(Error::NotEnoughData)
}

/// Reject a wire length above [`MAX_STRING_SIZE`], comparing in `u64` so a
/// 32-bit host cannot truncate its way past the cap.
fn checked_string_len(len: u64) -> Result<u64> {
    if len > MAX_STRING_SIZE {
        return Err(Error::BadResponse(format!(
            "native protocol: string too large: {len} > {MAX_STRING_SIZE}"
        )));
    }
    Ok(len)
}

/// [`checked_string_len`] narrowed to an allocation size.
fn string_len_usize(len: u64) -> Result<usize> {
    usize::try_from(checked_string_len(len)?).map_err(|_| {
        Error::BadResponse(format!(
            "native protocol: string length {len} exceeds platform usize"
        ))
    })
}

/// Empty `Vec<T>` with room for `cap` elements, allocated fallibly because
/// `cap` derives from a server-controlled count.
///
/// # Errors
///
/// As [`zeroed`].
pub(crate) fn with_cap<T>(cap: usize) -> Result<Vec<T>> {
    let mut buf = Vec::new();
    buf.try_reserve_exact(cap)
        .map_err(|_| alloc_refused(cap.saturating_mul(std::mem::size_of::<T>())))?;
    Ok(buf)
}

fn alloc_refused(bytes: usize) -> Error {
    Error::BadResponse(format!(
        "native protocol: refused a {bytes}-byte column buffer the allocator could not back"
    ))
}

/// Bytes claimed up front by [`read_exact_grown`] before any arrive.
const READ_CHUNK: usize = 64 * 1024;

/// Read exactly `len` bytes, growing the buffer as they arrive.
///
/// A declared length is not evidence the bytes exist, so the buffer commits
/// memory only in proportion to what the stream delivers: a short or hostile
/// length costs one chunk, not its full claim.
///
/// # Errors
///
/// [`Error::BadResponse`] when the allocator cannot back a chunk; I/O errors,
/// an unexpected EOF included, propagate.
pub(crate) async fn read_exact_grown<R: AsyncRead + Unpin>(
    reader: &mut R,
    len: usize,
) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    let mut left = len;
    while left > 0 {
        let chunk = left.min(READ_CHUNK);
        let start = buf.len();
        buf.try_reserve(chunk)
            .map_err(|_| alloc_refused(start.saturating_add(chunk)))?;
        buf.resize(start + chunk, 0);
        reader.read_exact(&mut buf[start..]).await?;
        left -= chunk;
    }
    Ok(buf)
}

/// Extension trait on `AsyncRead` for `ClickHouse` wire protocol.
///
/// No `Sync` bound: the returned futures only need `&mut Self` to be `Send`,
/// and upstream's `BytesCursor` -- the reader behind `Query::fetch_bytes`, so
/// the HTTP half of the codec -- is `Send` but not `Sync`.
pub(crate) trait ClickHouseRead: AsyncRead + Unpin + Send {
    fn read_var_uint(&mut self) -> impl Future<Output = Result<u64>> + Send + '_;

    fn read_string(&mut self) -> impl Future<Output = Result<Vec<u8>>> + Send + '_;

    fn read_utf8_string(&mut self) -> impl Future<Output = Result<String>> + Send + '_ {
        async {
            let bytes = self.read_string().await?;
            String::from_utf8(bytes)
                .map_err(|e| Error::BadResponse(format!("native protocol: invalid utf8: {e}")))
        }
    }
}

/// A varuint, or `None` when the stream ended cleanly on a block boundary.
///
/// A `FORMAT Native` body is a bare run of blocks with no terminator, so the
/// only way to know it is finished is that the next block's first byte never
/// arrives. Ending part-way through a value is still an error.
pub(crate) async fn read_var_uint_or_eof<R: ClickHouseRead>(r: &mut R) -> Result<Option<u64>> {
    let mut first = [0u8; 1];
    if r.read(&mut first).await? == 0 {
        return Ok(None);
    }
    let mut decoder = VarUintDecoder::default();
    if let Some(value) = decoder.push(first[0])? {
        return Ok(Some(value));
    }
    loop {
        let mut octet = [0u8; 1];
        r.read_exact(&mut octet[..]).await?;
        if let Some(value) = decoder.push(octet[0])? {
            return Ok(Some(value));
        }
    }
}

impl<T: AsyncRead + Unpin + Send> ClickHouseRead for T {
    async fn read_var_uint(&mut self) -> Result<u64> {
        let mut decoder = VarUintDecoder::default();
        loop {
            let mut octet = [0u8];
            self.read_exact(&mut octet[..]).await?;
            if let Some(value) = decoder.push(octet[0])? {
                return Ok(value);
            }
        }
    }

    async fn read_string(&mut self) -> Result<Vec<u8>> {
        let len = string_len_usize(self.read_var_uint().await?)?;
        read_exact_grown(self, len).await
    }
}

/// Extension trait on `AsyncWrite` for `ClickHouse` wire protocol.
pub(crate) trait ClickHouseWrite: AsyncWrite + Unpin + Send + Sync {
    fn write_var_uint(&mut self, value: u64) -> impl Future<Output = Result<()>> + Send + '_;

    fn write_string<V: AsRef<[u8]> + Send>(
        &mut self,
        value: V,
    ) -> impl Future<Output = Result<()>> + Send + use<'_, Self, V>;
}

impl<T: AsyncWrite + Unpin + Send + Sync> ClickHouseWrite for T {
    async fn write_var_uint(&mut self, value: u64) -> Result<()> {
        let mut buf = VarUintBuf::default();
        let len = encode_var_uint(value, &mut buf);
        self.write_all(&buf[..len]).await?;
        Ok(())
    }

    async fn write_string<V: AsRef<[u8]> + Send>(&mut self, value: V) -> Result<()> {
        let value = value.as_ref();
        self.write_var_uint(value.len() as u64).await?;
        self.write_all(value).await?;
        Ok(())
    }
}

/// Sync extension trait on `bytes::Buf` for `ClickHouse` wire protocol.
///
/// Bound of the sync sparse reader, which only tests drive today.
#[allow(dead_code)]
pub(crate) trait ClickHouseBytesRead: bytes::Buf {
    fn try_get_var_uint(&mut self) -> Result<u64>;
    fn try_get_string(&mut self) -> Result<bytes::Bytes>;
}

impl<T: bytes::Buf> ClickHouseBytesRead for T {
    #[inline]
    fn try_get_var_uint(&mut self) -> Result<u64> {
        let mut decoder = VarUintDecoder::default();
        loop {
            if !self.has_remaining() {
                return Err(Error::NotEnoughData);
            }
            if let Some(value) = decoder.push(self.get_u8())? {
                return Ok(value);
            }
        }
    }

    #[inline]
    fn try_get_string(&mut self) -> Result<bytes::Bytes> {
        let len = string_len_usize(self.try_get_var_uint()?)?;

        if len == 0 {
            return Ok(bytes::Bytes::new());
        }

        if self.remaining() < len {
            return Err(Error::NotEnoughData);
        }

        Ok(self.copy_to_bytes(len))
    }
}

/// Sync extension trait on `bytes::BufMut` for `ClickHouse` wire protocol.
pub trait ClickHouseBytesWrite: bytes::BufMut {
    /// Append `value` as a `VarUInt`.
    fn put_var_uint(&mut self, value: u64);
    /// Append `value` as a `VarUInt` length followed by its bytes.
    fn put_string<V: AsRef<[u8]>>(&mut self, value: V);
}

impl<T: bytes::BufMut> ClickHouseBytesWrite for T {
    fn put_var_uint(&mut self, value: u64) {
        let mut buf = VarUintBuf::default();
        let len = encode_var_uint(value, &mut buf);
        self.put_slice(&buf[..len]);
    }

    fn put_string<V: AsRef<[u8]>>(&mut self, value: V) {
        let value = value.as_ref();
        self.put_var_uint(value.len() as u64);
        self.put_slice(value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{Bytes, BytesMut};

    #[test]
    fn test_var_uint_roundtrip_sync() {
        // ClickHouse varint packs 7 value bits per byte, up to 10 bytes
        // for the full u64 range. u64::MAX exercises the 10-byte path.
        let test_values: &[u64] = &[
            0,
            1,
            127,
            128,
            255,
            256,
            16383,
            16384,
            (1 << 63) - 1,
            1 << 63,
            u64::MAX,
        ];
        for &val in test_values {
            let mut buf = BytesMut::new();
            buf.put_var_uint(val);
            let mut reader = buf.freeze();
            let decoded = reader.try_get_var_uint().unwrap();
            assert_eq!(val, decoded, "roundtrip failed for {val}");
        }
    }

    #[test]
    fn test_string_roundtrip_sync() {
        let test_strings: &[&[u8]] = &[b"", b"hello", b"hello world", &[0u8; 1000]];
        for &val in test_strings {
            let mut buf = BytesMut::new();
            buf.put_string(val);
            let mut reader = buf.freeze();
            let decoded = reader.try_get_string().unwrap();
            assert_eq!(val, &decoded[..], "roundtrip failed");
        }
    }

    #[tokio::test]
    async fn test_var_uint_roundtrip_async() {
        let test_values: &[u64] = &[
            0,
            1,
            127,
            128,
            255,
            256,
            16383,
            16384,
            (1 << 63) - 1,
            1 << 63,
            u64::MAX,
        ];
        for &val in test_values {
            let mut buf = Vec::new();
            buf.write_var_uint(val).await.unwrap();
            let mut reader = std::io::Cursor::new(buf);
            let decoded = reader.read_var_uint().await.unwrap();
            assert_eq!(val, decoded, "async roundtrip failed for {val}");
        }
    }

    #[tokio::test]
    async fn test_string_roundtrip_async() {
        let test_strings: &[&[u8]] = &[b"", b"hello", b"hello world"];
        for &val in test_strings {
            let mut buf = Vec::new();
            buf.write_string(val).await.unwrap();
            let mut reader = std::io::Cursor::new(buf);
            let decoded = reader.read_string().await.unwrap();
            assert_eq!(val, &decoded[..], "async roundtrip failed");
        }
    }

    #[test]
    fn test_eof_returns_error() {
        let mut buf = Bytes::new();
        let result = buf.try_get_var_uint();
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_var_uint_malformed_too_long_async() {
        // A varint whose continuation bit never clears must fail closed
        // at the 10-byte ceiling rather than truncate the value or spin
        // -- otherwise a corrupt/hostile stream desyncs every later
        // read. 12 bytes, all with the high bit set.
        let malformed = vec![0x81u8; 12];
        let mut reader = std::io::Cursor::new(malformed);
        let err = reader
            .read_var_uint()
            .await
            .expect_err("over-long varint must be rejected");
        assert!(
            matches!(err, Error::BadResponse(_)),
            "expected BadResponse, got {err:?}"
        );
        assert!(err.to_string().contains("past 10 bytes"), "got: {err}");
    }

    #[test]
    fn string_len_boundary_is_inclusive() {
        assert_eq!(
            checked_string_len(MAX_STRING_SIZE).expect("the cap itself is accepted"),
            MAX_STRING_SIZE
        );
        let err = checked_string_len(MAX_STRING_SIZE + 1).expect_err("one past the cap rejects");
        assert!(err.to_string().contains("string too large"), "{err}");
    }

    #[test]
    fn get_var_uint_shares_the_ten_byte_ceiling() {
        assert_eq!(get_var_uint(&[0x00]).expect("zero decodes"), (0, 1));
        assert_eq!(get_var_uint(&[0xAC, 0x02]).expect("decodes"), (300, 2));
        // A value that never clears its continuation bit fails at the ceiling,
        // the same place the two stream readers stop.
        let err = get_var_uint(&[0x81u8; 12]).expect_err("an over-long varint rejects");
        assert!(err.to_string().contains("past 10 bytes"), "{err}");
        // Running out of bytes mid-value is a distinct, recoverable outcome.
        assert!(matches!(
            get_var_uint(&[0x81, 0x81]),
            Err(Error::NotEnoughData)
        ));
    }

    #[test]
    fn test_var_uint_malformed_too_long_sync() {
        // Same fail-closed guarantee on the sync `bytes::Buf` reader.
        let malformed = vec![0x81u8; 12];
        let mut reader = Bytes::from(malformed);
        let err = reader
            .try_get_var_uint()
            .expect_err("over-long varint must be rejected");
        assert!(
            matches!(err, Error::BadResponse(_)),
            "expected BadResponse, got {err:?}"
        );
        assert!(err.to_string().contains("past 10 bytes"), "got: {err}");
    }
}
