use data_seeds::cast::{self, Error, Format};

#[test]
fn integers_preserve_full_width_and_reject_truncation() {
    assert_eq!(cast::uint(&u128::MAX.to_string()), Ok(u128::MAX));
    assert_eq!(cast::int(&i128::MIN.to_string()), Ok(i128::MIN));
    assert_eq!(cast::int(&i128::MAX.to_string()), Ok(i128::MAX));
    assert_eq!(cast::uint("9007199254740993"), Ok(9_007_199_254_740_993));
    assert_eq!(cast::int("  +12  "), Ok(12));
    assert_eq!(cast::int("-0"), Ok(0));
    assert_eq!(cast::uint("-1"), Err(Error::Unsigned));
    assert_eq!(
        cast::int("170141183460469231731687303715884105728"),
        Err(Error::Overflow)
    );
    assert_eq!(
        cast::int("-170141183460469231731687303715884105729"),
        Err(Error::Overflow)
    );
    assert_eq!(
        cast::uint("340282366920938463463374607431768211456"),
        Err(Error::Overflow)
    );
    for s in [
        "1.5", "1e3", "1,000", "1_000", "0xff", "12ms", "$12", "1 2", "１２",
    ] {
        assert!(cast::int(s).is_err(), "accepted {s}");
    }
}

#[test]
fn declared_affixes_are_matched_without_unit_or_currency_conversion() {
    for symbol in ["$", "¥", "£", "€"] {
        let format = Format::new().prefix(symbol);
        assert_eq!(format.float(&format!(" {symbol}12.50 ")), Ok(12.5));
        assert_eq!(format.float(&format!("-{symbol}12.50")), Ok(-12.5));
        assert_eq!(format.float(&format!("{symbol}-12.50")), Ok(-12.5));
        assert_eq!(format.uint(&format!("{symbol} 12")), Ok(12));
        assert_eq!(format.uint("12"), Err(Error::Prefix));
    }
    for unit in ["s", "ms", "ft"] {
        let format = Format::new().suffix(unit);
        assert_eq!(format.int(&format!("12{unit}")), Ok(12));
        assert_eq!(format.int(&format!("12 {unit}")), Ok(12));
        assert_eq!(format.float(&format!("-1.25 {unit}")), Ok(-1.25));
        assert_eq!(format.int("12"), Err(Error::Suffix));
    }
    assert!(Format::new().suffix("s").float("12ms").is_err());
    assert!(Format::new().suffix("ms").float("12s").is_err());
    assert!(Format::new().prefix("$").float("€12").is_err());
    assert_eq!(
        Format::new()
            .prefix("USD")
            .suffix("/s")
            .float("USD 12.5 /s"),
        Ok(12.5)
    );
}

#[test]
fn malformed_values_and_nonfinite_floats_fail_explicitly() {
    assert_eq!(cast::float("1.25e3"), Ok(1250.0));
    assert_eq!(cast::float(".5"), Ok(0.5));
    assert!(cast::float("-0").unwrap().is_sign_negative());
    for s in ["NaN", "inf", "-inf", "1e9999"] {
        assert_eq!(cast::float(s), Err(Error::NonFinite));
    }
    for s in ["", "   "] {
        assert_eq!(cast::float(s), Err(Error::Empty));
    }
    for s in [
        "--1", "+-1", "++1", "-+1", "- 1", "1,234.50", "1.234,50", "12ms3", "12%",
    ] {
        assert!(cast::float(s).is_err(), "accepted {s}");
    }
    let money = Format::new().prefix("$");
    assert_eq!(money.float("-$-1"), Err(Error::Sign));
    assert!(money.float("$$1").is_err());
    assert!(money.float("$1$").is_err());
    assert_eq!(money.float("$"), Err(Error::Empty));
    assert_eq!(Format::new().suffix("ms").float("ms"), Err(Error::Empty));
}
