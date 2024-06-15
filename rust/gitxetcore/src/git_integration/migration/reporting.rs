#[derive(Default)]
pub struct OIDIssueTracker {
    issues: HashMap<Oid, (bool, String)>,
}

impl OIDIssueTracker {
    pub fn rec_ignored(&mut self, oid: Oid, msg: String) {
        tr_print!("IGNORED  {oid}: {msg}");
        match issues.entry(oid) {}
    }

    pub fn rec_passthrough(&mut self, oid: Oid, msg: String) {
        tr_print!("PASSTHRU {oid}: {msg}");
    }
}
