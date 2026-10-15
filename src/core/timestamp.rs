//! Calendar conversion for TIMESTAMP values.
//!
//! A timestamp is a signed count of whole seconds since 1970-01-01 00:00:00, with no time zone.
//! Dates are converted with Howard Hinnant's `days_from_civil` and `civil_from_days` algorithms,
//! which are exact for the proleptic Gregorian calendar.

/// Number of seconds in a day.
pub(crate) const SECONDS_PER_DAY: i64 = 86_400;

/// Returns the number of days from 1970-01-01 to the given civil date (negative before it).
///
/// `month` is 1-12 and `day` is 1-31; the caller must validate them.
pub(crate) fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let year_of_era = y - era * 400;
    // March is month 0 of the civil year, so February (the last month) comes last.
    let month_index = (month + 9) % 12;
    let day_of_year = (153 * month_index + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

/// Returns the civil `(year, month, day)` for a count of days since 1970-01-01.
///
/// This is the inverse of [`days_from_civil`].
pub(crate) fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let shifted = days + 719_468;
    let era = shifted.div_euclid(146_097);
    let day_of_era = shifted - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_index = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_index + 2) / 5 + 1;
    let month = if month_index < 10 {
        month_index + 3
    } else {
        month_index - 9
    };
    let year = year_of_era + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

/// Returns true if `year` is a leap year in the Gregorian calendar.
pub(crate) fn is_leap_year(year: i64) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}

/// Returns the number of days in `month` of `year`, or 0 if `month` is not in 1-12.
pub(crate) fn days_in_month(year: i64, month: i64) -> i64 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => 0,
    }
}

/// Parses exactly `len` ASCII digits from `part`, returning `None` for anything else.
fn digits(part: &str, len: usize) -> Option<i64> {
    if part.len() != len || !part.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    part.parse().ok()
}

/// Parses `YYYY-MM-DD` with an optional `HH:MM:SS` part into seconds since the epoch.
///
/// The date and time may be separated by a space or `T`. Surrounding whitespace is ignored, and
/// a date with no time means midnight. Years must be 0001-9999 and every field must be in range
/// for its calendar month, so `2026-02-30` and `2023-02-29` are rejected.
///
/// # Errors
///
/// Returns `invalid input syntax for type timestamp` for malformed text, and
/// `date/time field value out of range` for well-formed text with an impossible date or time.
pub(crate) fn parse_timestamp(text: &str) -> Result<i64, String> {
    let syntax_error = || format!("invalid input syntax for type timestamp: \"{text}\"");
    let range_error = || format!("date/time field value out of range: \"{text}\"");

    let trimmed = text.trim();
    let (date_part, time_part) = match trimmed.split_once([' ', 'T']) {
        Some((date, time)) => (date, Some(time)),
        None => (trimmed, None),
    };

    let date_fields: Vec<&str> = date_part.split('-').collect();
    let [year_text, month_text, day_text] = date_fields.as_slice() else {
        return Err(syntax_error());
    };
    let (Some(year), Some(month), Some(day)) = (
        digits(year_text, 4),
        digits(month_text, 2),
        digits(day_text, 2),
    ) else {
        return Err(syntax_error());
    };

    let (mut hour, mut minute, mut second) = (0, 0, 0);
    if let Some(time) = time_part {
        let time_fields: Vec<&str> = time.split(':').collect();
        let [hour_text, minute_text, second_text] = time_fields.as_slice() else {
            return Err(syntax_error());
        };
        let (Some(h), Some(m), Some(s)) = (
            digits(hour_text, 2),
            digits(minute_text, 2),
            digits(second_text, 2),
        ) else {
            return Err(syntax_error());
        };
        (hour, minute, second) = (h, m, s);
    }

    if !(1..=9999).contains(&year)
        || !(1..=12).contains(&month)
        || !(1..=days_in_month(year, month)).contains(&day)
        || hour > 23
        || minute > 59
        || second > 59
    {
        return Err(range_error());
    }

    let days = days_from_civil(year, month, day);
    Ok(days * SECONDS_PER_DAY + hour * 3600 + minute * 60 + second)
}

/// Formats seconds since the epoch as `YYYY-MM-DD HH:MM:SS`.
///
/// Times before 1970 are handled with Euclidean division, so the time of day stays in
/// 00:00:00-23:59:59 and the date moves back one day, as expected.
pub(crate) fn format_timestamp(seconds: i64) -> String {
    let days = seconds.div_euclid(SECONDS_PER_DAY);
    let second_of_day = seconds.rem_euclid(SECONDS_PER_DAY);
    let (year, month, day) = civil_from_days(days);
    let (hour, minute, second) = (
        second_of_day / 3600,
        second_of_day % 3600 / 60,
        second_of_day % 60,
    );
    format!("{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_timestamp_known_values() {
        assert_eq!(parse_timestamp("1970-01-01 00:00:00"), Ok(0));
        assert_eq!(parse_timestamp("1969-12-31 23:59:59"), Ok(-1));
        assert_eq!(parse_timestamp("2000-01-01 00:00:00"), Ok(946_684_800));
        assert_eq!(parse_timestamp("2026-03-01 12:00:00"), Ok(1_772_366_400));
        assert_eq!(parse_timestamp("0001-01-01 00:00:00"), Ok(-62_135_596_800));
        assert_eq!(parse_timestamp("9999-12-31 23:59:59"), Ok(253_402_300_799));
    }

    #[test]
    fn test_parse_timestamp_date_only_is_midnight() {
        assert_eq!(
            parse_timestamp("2026-01-01"),
            parse_timestamp("2026-01-01 00:00:00")
        );
        assert_eq!(parse_timestamp("2026-01-01"), Ok(1_767_225_600));
    }

    #[test]
    fn test_parse_timestamp_accepts_t_separator_and_outer_spaces() {
        let expected = parse_timestamp("2026-03-01 12:00:00");
        assert_eq!(parse_timestamp("2026-03-01T12:00:00"), expected);
        assert_eq!(parse_timestamp("  2026-03-01 12:00:00  "), expected);
        assert_eq!(parse_timestamp("\t2026-03-01T12:00:00\n"), expected);
    }

    #[test]
    fn test_parse_timestamp_rejects_malformed_text() {
        for text in [
            "",
            "yesterday",
            "2026/01/01",
            "2026-1-01",
            "26-01-01",
            "20260101",
            "+2026-01-01",
            "2026-01-01 12:00",
            "2026-01-01 12:00:00.5",
            "2026-01-01 12-00-00",
            "2026-01-01  12:00:00",
            "2026-01-01 12:00:00 extra",
            "2026-01-01 1x:00:00",
        ] {
            let error = parse_timestamp(text).unwrap_err();
            assert!(
                error.starts_with("invalid input syntax for type timestamp"),
                "{text:?} gave {error:?}"
            );
        }
    }

    #[test]
    fn test_parse_timestamp_rejects_fields_out_of_range() {
        for text in [
            "2026-02-30",
            "2026-04-31",
            "2026-13-01",
            "2026-00-10",
            "2026-01-00",
            "0000-01-01",
            "2026-01-01 24:00:00",
            "2026-01-01 12:60:00",
            "2026-01-01 12:00:60",
        ] {
            let error = parse_timestamp(text).unwrap_err();
            assert!(
                error.starts_with("date/time field value out of range"),
                "{text:?} gave {error:?}"
            );
        }
    }

    #[test]
    fn test_parse_timestamp_leap_years() {
        assert!(parse_timestamp("2024-02-29").is_ok());
        assert!(parse_timestamp("2000-02-29").is_ok());
        assert!(parse_timestamp("2400-02-29").is_ok());
        assert_eq!(parse_timestamp("2400-02-29"), Ok(13_574_563_200));
        assert!(parse_timestamp("2023-02-29").is_err());
        assert!(parse_timestamp("1900-02-29").is_err());
        assert!(parse_timestamp("2100-02-29").is_err());
        assert!(parse_timestamp("2024-02-30").is_err());
        assert_eq!(days_in_month(1900, 2), 28);
        assert_eq!(days_in_month(2000, 2), 29);
        assert_eq!(days_in_month(2026, 12), 31);
        assert_eq!(days_in_month(2026, 13), 0);
        // The day after Feb 28 in a leap year is Feb 29, and then March 1.
        assert_eq!(
            parse_timestamp("2024-03-01").unwrap() - parse_timestamp("2024-02-28").unwrap(),
            2 * SECONDS_PER_DAY
        );
        assert_eq!(
            format_timestamp(parse_timestamp("2024-02-29 06:30:00").unwrap()),
            "2024-02-29 06:30:00"
        );
    }

    #[test]
    fn test_format_timestamp_known_values() {
        assert_eq!(format_timestamp(0), "1970-01-01 00:00:00");
        assert_eq!(format_timestamp(-1), "1969-12-31 23:59:59");
        assert_eq!(format_timestamp(1_772_366_400), "2026-03-01 12:00:00");
        assert_eq!(format_timestamp(-62_135_596_800), "0001-01-01 00:00:00");
        assert_eq!(format_timestamp(253_402_300_799), "9999-12-31 23:59:59");
        assert_eq!(format_timestamp(1_904_281_689), "2030-05-06 07:08:09");
    }

    #[test]
    fn test_format_timestamp_round_trips_through_parse() {
        for text in [
            "1970-01-01 00:00:00",
            "1969-12-31 23:59:59",
            "1900-03-01 00:00:01",
            "2000-02-29 23:59:59",
            "2026-03-01 12:00:00",
            "0001-01-01 00:00:00",
            "9999-12-31 23:59:59",
        ] {
            let seconds = parse_timestamp(text).unwrap();
            assert_eq!(format_timestamp(seconds), text);
            assert_eq!(parse_timestamp(&format_timestamp(seconds)), Ok(seconds));
        }
    }
}
