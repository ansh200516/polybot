//! Exact decimal-string parsing. "0.46" → 460_000 µ. Never touches f64.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DecimalError {
    Empty,
    BadChar,
    TooManyDecimals,
    Overflow,
}

impl std::fmt::Display for DecimalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DecimalError::Empty => write!(f, "decimal string is empty"),
            DecimalError::BadChar => write!(f, "decimal string contains invalid characters"),
            DecimalError::TooManyDecimals => {
                write!(f, "decimal string has more than 6 fractional digits")
            }
            DecimalError::Overflow => write!(f, "decimal value overflows u64"),
        }
    }
}

impl std::error::Error for DecimalError {}

/// Parse a non-negative decimal string to µ units (×10⁶), exactly.
pub fn parse_micro(s: &str) -> Result<u64, DecimalError> {
    let (int_part, frac_part) = match s.split_once('.') {
        Some((i, f)) => (i, f),
        None => (s, ""),
    };
    if int_part.is_empty() && frac_part.is_empty() {
        return Err(DecimalError::Empty);
    }
    if frac_part.len() > 6 {
        return Err(DecimalError::TooManyDecimals);
    }
    let mut value: u64 = 0;
    if !int_part.is_empty() {
        if !int_part.bytes().all(|b| b.is_ascii_digit()) {
            return Err(DecimalError::BadChar);
        }
        for b in int_part.bytes() {
            value = value
                .checked_mul(10)
                .and_then(|v| v.checked_add(u64::from(b - b'0')))
                .ok_or(DecimalError::Overflow)?;
        }
    }
    value = value.checked_mul(1_000_000).ok_or(DecimalError::Overflow)?;
    if !frac_part.is_empty() {
        if !frac_part.bytes().all(|b| b.is_ascii_digit()) {
            return Err(DecimalError::BadChar);
        }
        let mut frac: u64 = 0;
        for b in frac_part.bytes() {
            frac = frac * 10 + u64::from(b - b'0'); // ≤ 6 digits: cannot overflow
        }
        // Sound: frac_part.len() ≤ 6 is enforced by the TooManyDecimals guard above.
        frac *= 10u64.pow(6 - frac_part.len() as u32);
        value = value.checked_add(frac).ok_or(DecimalError::Overflow)?;
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn golden_values() {
        assert_eq!(parse_micro("0.46").unwrap(), 460_000);
        assert_eq!(parse_micro("0.001").unwrap(), 1_000);
        assert_eq!(parse_micro("1").unwrap(), 1_000_000);
        assert_eq!(parse_micro("0").unwrap(), 0);
        assert_eq!(parse_micro("12.5").unwrap(), 12_500_000);
        assert_eq!(parse_micro("0.000001").unwrap(), 1);
        assert_eq!(parse_micro("123456.654321").unwrap(), 123_456_654_321);
        assert_eq!(parse_micro(".5").unwrap(), 500_000); // venue sometimes omits leading zero
        assert_eq!(parse_micro("7.").unwrap(), 7_000_000);
    }

    #[test]
    fn rejects_garbage() {
        for bad in [
            "",
            ".",
            "-1",
            "+1",
            "1e3",
            "0.0000001",
            "1.2.3",
            "abc",
            "0x10",
            " 1",
            "1 ",
        ] {
            assert!(parse_micro(bad).is_err(), "{bad:?} must be rejected");
        }
    }

    #[test]
    fn rejects_overflow() {
        assert!(parse_micro("18446744073709551616").is_err());
        assert!(parse_micro("99999999999999999999.0").is_err());
    }

    proptest! {
        #[test]
        fn roundtrips_canonical(micro in 0u64..1_000_000_000_000) {
            let int = micro / 1_000_000;
            let frac = micro % 1_000_000;
            let s = if frac == 0 { format!("{int}") } else {
                let f = format!("{frac:06}");
                format!("{int}.{}", f.trim_end_matches('0'))
            };
            prop_assert_eq!(parse_micro(&s).unwrap(), micro);
        }
    }
}
