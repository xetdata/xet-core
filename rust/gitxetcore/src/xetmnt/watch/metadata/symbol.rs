use nfsserve::nfs::{filename3, nfsstat3};
use intaglio::Symbol;
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use intaglio::osstr::SymbolTable;
use std::ffi::{OsStr, OsString};
use nfsserve::nfs::nfsstat3::{NFS3ERR_INVAL, NFS3ERR_IO};
use std::str::FromStr;
use crate::log::ErrorPrinter;

/// A thread-safe symbol table. This table will intern UTF8-compatible [filename3]s into
/// [Symbol]s that can be more efficiently managed.
pub struct Symbols {
    table: RwLock<SymbolTable>,
}

impl Symbols {

    pub fn new() -> Self {
        Self {
            table: RwLock::new(SymbolTable::new()),
        }
    }

    /// Get the default symbol (i.e. symbol for "").
    pub fn default_symbol(&self) -> Result<Symbol, nfsstat3> {
        let default_str = OsString::default();
        self.lock_write()
            .and_then(|mut table| table.intern(default_str)
                .map_err(|_|NFS3ERR_IO))
    }

    /// Encode the filename into a Symbol. Will return an [NFS3ERR_INVAL] if the
    /// filename is not valid UTF-8.
    pub fn encode_symbol(&self, name: &filename3) -> Result<Symbol, nfsstat3> {
        let os_str = Self::filename_to_os_string(name)?;
        self.lock_write()
            .and_then(|mut table| table.intern(os_str).map_err(|_| NFS3ERR_IO))
    }

    /// Decode the given Symbol as a filename.
    /// Returns an [NFS3ERR_IO] if the Symbol is not found.
    pub fn decode_symbol(&self, sym: Symbol) -> Result<filename3, nfsstat3> {
        self.lock_read()?
            .get(sym)
            .and_then(OsStr::to_str)
            .map(str::as_bytes)
            .map(filename3::from)
            .ok_or(NFS3ERR_IO)
    }

    /// Gets the Symbol for the given filename. If the filename isn't present,
    /// then Ok(None) is returned.
    /// Will return an [NFS3ERR_INVAL] if the filename is not valid UTF-8.
    pub fn get_symbol(&self, name: &filename3) -> Result<Option<Symbol>, nfsstat3> {
        let os_str = Self::filename_to_os_string(name)?;
        self.lock_read()
            .map(|table| table.check_interned(&os_str))
    }

    /// Converts the filename to an OsString, retunrning [NFS3ERR_INVAL] if the filename
    /// isn't valid UTF-8.
    fn filename_to_os_string(name: &filename3) -> Result<OsString, nfsstat3> {
        std::str::from_utf8(name)
            .map_err(|_| NFS3ERR_INVAL)
            .and_then(|s| OsString::from_str(s).map_err(|_| NFS3ERR_INVAL))
    }

    /// Lock the table for reads
    fn lock_read(&self) -> Result<RwLockReadGuard<'_, SymbolTable>, nfsstat3> {
        self.table
            .read()
            .log_error("Couldn't open Symbols lock for read")
            .map_err(|_| NFS3ERR_IO)
    }

    /// Lock the table for writes
    fn lock_write(&self) -> Result<RwLockWriteGuard<'_, SymbolTable>, nfsstat3> {
        self.table
            .write()
            .log_error("Couldn't open Symbols lock for write")
            .map_err(|_| NFS3ERR_IO)
    }
}

#[cfg(test)]
mod symbol_tests {
    use intaglio::Symbol;
    use nfsserve::nfs::filename3;
    use nfsserve::nfs::nfsstat3::{NFS3ERR_INVAL, NFS3ERR_IO};
    use super::Symbols;

    // TODO: have filename3 impl From<&str> (requires external repo/crate changes)
    fn to_filename(s: &str) -> filename3 {
        s.as_bytes().into()
    }

    #[test]
    fn test_default_symbol() {
        let table = Symbols::new();
        let sym = table.default_symbol().unwrap();
        let sym2 = table.default_symbol().unwrap();
        assert_eq!(sym, sym2);
    }

    #[test]
    fn test_encode_decode() {
        let table = Symbols::new();

        let w1 = to_filename("file1.txt");
        let w2 = to_filename("dir-2");
        let w3 = to_filename("file1.txt");
        let s1 = table.encode_symbol(&w1).unwrap();
        let s2 = table.encode_symbol(&w2).unwrap();
        let s3 = table.encode_symbol(&w3).unwrap();

        assert_eq!(s1, s3); // symbol table should remove duplicates.

        // check symbols are present
        let f1 = table.get_symbol(&w1).unwrap().unwrap();
        assert_eq!(s1, f1);
        let f2 = table.get_symbol(&w2).unwrap().unwrap();
        assert_eq!(s2, f2);

        // check decoding of symbols
        let f1 = table.decode_symbol(s1).unwrap();
        assert_eq!(w1.as_ref(), f1.as_ref());
        let f2 = table.decode_symbol(s2).unwrap();
        assert_eq!(w2.as_ref(), f2.as_ref());
    }

    #[test]
    fn test_error() {
        let table = Symbols::new();
        // Invalid UTF-8 filename
        let filename = vec![0, 159, 146, 150].into();
        let err = table.encode_symbol(&filename).unwrap_err();
        assert!(matches!(err, NFS3ERR_INVAL));
        let err = table.get_symbol(&filename).unwrap_err();
        assert!(matches!(err, NFS3ERR_INVAL));

        // Non-existent symbol to decode.
        let err = table.decode_symbol(Symbol::new(5372)).unwrap_err();
        assert!(matches!(err, NFS3ERR_IO));
    }

    #[test]
    fn test_not_found() {
        let table = Symbols::new();
        let filename = to_filename("not_found");
        let maybe_symbol = table.get_symbol(&filename).unwrap();
        assert!(maybe_symbol.is_none());
    }

}
