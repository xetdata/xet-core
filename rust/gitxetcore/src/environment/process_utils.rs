use std;
use tokio;

pub trait CommandSanitized {
    fn new_sanitized(program: &str) -> Self;
}

impl CommandSanitized for std::process::Command {
    fn new_sanitized(program: &str) -> Self {
        let mut command = std::process::Command::new(program);
        command.env("LD_PRELOAD", "");
        command.env("DYLD_INSERT_LIBRARIES", "");
        command
    }
}

impl CommandSanitized for tokio::process::Command {
    fn new_sanitized(program: &str) -> Self {
        let mut command = tokio::process::Command::new(program);
        command.env("LD_PRELOAD", "");
        command.env("DYLD_INSERT_LIBRARIES", "");
        command
    }
}
