//! Exact decimal string -> fixed point (1_000_000 = 1.0). No floats: f64 would
//! turn "0.29" into 289_999.

pub const SCALE: i64 = 1_000_000;

/// Parses a non-negative decimal like "0.29", ".5" or "12". Digits beyond the
/// 6th decimal are truncated. Returns None for anything else.
pub fn parse(s: &str) -> Option<i64> {
    let (int, frac) = s.split_once('.').unwrap_or((s, ""));
    let digits = |p: &str| p.bytes().all(|b| b.is_ascii_digit());
    if (int.is_empty() && frac.is_empty()) || !digits(int) || !digits(frac) {
        return None;
    }
    let int: i64 = if int.is_empty() { 0 } else { int.parse().ok()? };
    let mut frac_value = 0;
    let mut place = SCALE / 10;
    for b in frac.bytes().take(6) {
        frac_value += i64::from(b - b'0') * place;
        place /= 10;
    }
    int.checked_mul(SCALE)?.checked_add(frac_value)
}

#[cfg(test)]
mod tests {
    use super::parse;

    #[test]
    fn parses_exactly() {
        assert_eq!(parse("0.29"), Some(290_000));
        assert_eq!(parse(".5"), Some(500_000));
        assert_eq!(parse("12"), Some(12_000_000));
        assert_eq!(parse("1."), Some(1_000_000));
        assert_eq!(parse("0.001"), Some(1_000));
        assert_eq!(parse("1234.5678"), Some(1_234_567_800));
    }

    #[test]
    fn truncates_extra_decimals() {
        assert_eq!(parse("0.12345678"), Some(123_456));
    }

    #[test]
    fn rejects_bad_input() {
        for s in [
            "",
            ".",
            "1e5",
            "-1",
            "abc",
            "1.2.3",
            " 1",
            "99999999999999999999",
        ] {
            assert_eq!(parse(s), None, "{s:?}");
        }
    }
}
