use std::cmp::Ordering;
use std::fmt::{Display, Formatter};

/// Level of configuration. The order of priority (overriding) is:
/// ENV > LOCAL > GLOBAL > DEFAULT, meaning that if a variable is defined
/// in the LOCAL level, it has precedence over GLOBAL and DEFAULT, but not ENV.
#[derive(PartialEq, Eq, Debug, Copy, Clone)]
pub enum Level {
    ENV,
    LOCAL,
    GLOBAL,
    DEFAULT,
}

/// A sorted array of levels in order of increasing priority.
pub const LEVELS: [Level; 4] = [Level::DEFAULT, Level::GLOBAL, Level::LOCAL, Level::ENV];

impl Level {
    fn as_num(&self) -> u8 {
        match self {
            Level::ENV => 4,
            Level::LOCAL => 3,
            Level::GLOBAL => 2,
            Level::DEFAULT => 1,
        }
    }
}

impl PartialOrd for Level {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Level {
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_num().cmp(&other.as_num())
    }
}

impl Display for Level {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

#[cfg(test)]
mod level_tests {
    use crate::level::Level::{DEFAULT, ENV, GLOBAL, LOCAL};

    #[test]
    fn test_ordering() {
        let a = [DEFAULT, GLOBAL, LOCAL, ENV];

        for (i, x) in a.iter().enumerate() {
            for (j, y) in a.iter().enumerate() {
                assert_eq!(i.cmp(&j), x.cmp(y));
            }
        }
    }
}
