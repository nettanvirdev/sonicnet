//! The file name in a `Content-Disposition` header: RFC 6266, with RFC 8187 (née 5987)
//! `filename*`, and the RFC 2047 encoded-words mail gateways still put in `filename`.
//!
//! The name is untrusted and not yet fit for a file system; [`crate::sanitize_file_name`] makes
//! it one.

use percent_encoding::percent_decode_str;

/// The file name a `Content-Disposition` value gives, if it gives a non-empty one. `filename*`
/// wins over `filename` (RFC 6266 §4.3).
pub fn content_disposition_filename(value: &str) -> Option<String> {
    let params = parameters(value);
    let find = |key: &str| params.iter().find(|(k, _)| k.eq_ignore_ascii_case(key)).map(|(_, v)| v);
    if let Some(name) =
        find("filename*").and_then(|v| decode_ext_value(v)).filter(|n| !n.is_empty())
    {
        return Some(name);
    }
    let raw = find("filename")?;
    // Not in any RFC, but common: a percent-encoded name in plain `filename`.
    let decoded = percent_decode_str(raw).decode_utf8().map(|s| s.into_owned());
    let name = decode_rfc2047_filename(&decoded.unwrap_or_else(|_| raw.clone()));
    (!name.is_empty()).then_some(name)
}

/// The `name=value` parameters, quoted strings unescaped. Quotes protect `;` inside a value
/// (`filename="report; final.pdf"`). The disposition type, a bare token, is skipped.
fn parameters(value: &str) -> Vec<(String, String)> {
    let mut params = Vec::new();
    let mut rest = value;
    loop {
        rest = rest.trim_start_matches(|c: char| c == ';' || c.is_whitespace());
        if rest.is_empty() {
            return params;
        }
        let end = rest.find(['=', ';']).unwrap_or(rest.len());
        let name = rest[..end].trim().to_owned();
        rest = &rest[end..];
        let Some(after_equals) = rest.strip_prefix('=') else { continue };
        rest = after_equals.trim_start();
        let value = if let Some(quoted) = rest.strip_prefix('"') {
            let mut out = String::new();
            let mut escaped = false;
            // An unterminated quote runs to the end of the header.
            let mut consumed = quoted.len();
            for (i, c) in quoted.char_indices() {
                match c {
                    _ if escaped => {
                        out.push(c);
                        escaped = false;
                    }
                    '\\' => escaped = true,
                    '"' => {
                        consumed = i + 1;
                        break;
                    }
                    _ => out.push(c),
                }
            }
            rest = &quoted[consumed..];
            out
        } else {
            let end = rest.find(';').unwrap_or(rest.len());
            let token = rest[..end].trim().to_owned();
            rest = &rest[end..];
            token
        };
        params.push((name, value));
    }
}

/// `charset'language'percent-encoded` (RFC 8187 §3.2). UTF-8 and ISO-8859-1 are the charsets
/// the RFC requires; anything else is read as UTF-8 when it decodes as such.
fn decode_ext_value(value: &str) -> Option<String> {
    let mut parts = value.splitn(3, '\'');
    let (charset, _language, encoded) = (parts.next()?, parts.next()?, parts.next()?);
    let bytes: Vec<u8> = percent_decode_str(encoded).collect();
    if charset.eq_ignore_ascii_case("iso-8859-1") {
        // Latin-1 bytes are the first 256 code points.
        return Some(bytes.into_iter().map(char::from).collect());
    }
    Some(String::from_utf8(bytes).unwrap_or_else(|_| encoded.to_owned()))
}

/// Decodes a name made of RFC 2047 encoded-words (`=?charset?B|Q?text?=`), alone or mixed with
/// plain text. Whitespace between two adjacent encoded-words is dropped (§6.2); elsewhere it is
/// kept. If any encoded-word can't be decoded, the name is returned as it was.
pub fn decode_rfc2047_filename(name: &str) -> String {
    if !name.contains("=?") {
        return name.to_owned();
    }
    let trimmed = name.trim();
    let tokens = split_keeping_whitespace(trimmed);
    let mut out = String::new();
    let mut decoded_any = false;
    for (i, token) in tokens.iter().enumerate() {
        if token.starts_with(char::is_whitespace) {
            let between_words = i > 0
                && i + 1 < tokens.len()
                && is_encoded_word(tokens[i - 1])
                && is_encoded_word(tokens[i + 1]);
            if !between_words {
                out.push_str(token);
            }
        } else if is_encoded_word(token) {
            match decode_rfc2047_word(token) {
                Some(text) => {
                    out.push_str(&text);
                    decoded_any = true;
                }
                None => return name.to_owned(),
            }
        } else {
            out.push_str(token);
        }
    }
    if decoded_any { out } else { name.to_owned() }
}

/// Splits into runs of whitespace and runs of everything else, in order.
fn split_keeping_whitespace(text: &str) -> Vec<&str> {
    let mut tokens = Vec::new();
    let mut start = 0;
    let mut in_space = None;
    for (i, c) in text.char_indices() {
        let space = c.is_whitespace();
        if in_space.is_some_and(|s| s != space) {
            tokens.push(&text[start..i]);
            start = i;
        }
        in_space = Some(space);
    }
    if start < text.len() {
        tokens.push(&text[start..]);
    }
    tokens
}

fn encoded_word_parts(word: &str) -> Option<(&str, char, &str)> {
    let inner = word.strip_prefix("=?")?.strip_suffix("?=")?;
    let mut parts = inner.splitn(3, '?');
    let (charset, encoding, text) = (parts.next()?, parts.next()?, parts.next()?);
    let encoding = match encoding {
        "B" | "b" => 'B',
        "Q" | "q" => 'Q',
        _ => return None,
    };
    (!charset.is_empty() && !text.contains('?')).then_some((charset, encoding, text))
}

fn is_encoded_word(word: &str) -> bool {
    encoded_word_parts(word).is_some()
}

/// One encoded-word, or `None` when it is malformed or in a charset we can't decode — never a
/// guess, which would invent a name.
fn decode_rfc2047_word(word: &str) -> Option<String> {
    let (charset, encoding, text) = encoded_word_parts(word)?;
    let bytes = match encoding {
        'B' => decode_base64(text)?,
        _ => decode_q(text)?,
    };
    match charset.to_ascii_lowercase().as_str() {
        "utf-8" | "utf8" => String::from_utf8(bytes).ok(),
        "iso-8859-1" | "latin1" => Some(bytes.into_iter().map(char::from).collect()),
        "us-ascii" | "ascii" => {
            bytes.is_ascii().then(|| bytes.into_iter().map(char::from).collect())
        }
        _ => None,
    }
}

/// Q-encoding: `_` is a space, `=XX` a byte. A bare `=` is malformed.
fn decode_q(text: &str) -> Option<Vec<u8>> {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'_' => out.push(b' '),
            b'=' => {
                let hex = text.get(i + 1..i + 3)?;
                out.push(u8::from_str_radix(hex, 16).ok()?);
                i += 2;
            }
            b if b.is_ascii() => out.push(b),
            _ => return None,
        }
        i += 1;
    }
    Some(out)
}

/// Strict standard base64: the alphabet only, a whole number of quanta, padding only at the end.
fn decode_base64(text: &str) -> Option<Vec<u8>> {
    if !text.len().is_multiple_of(4) {
        return None;
    }
    let data = text.trim_end_matches('=');
    if text.len() - data.len() > 2 {
        return None;
    }
    let value = |c: u8| -> Option<u32> {
        Some(match c {
            b'A'..=b'Z' => c - b'A',
            b'a'..=b'z' => c - b'a' + 26,
            b'0'..=b'9' => c - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            _ => return None,
        } as u32)
    };
    let mut out = Vec::with_capacity(data.len() * 3 / 4);
    let (mut acc, mut bits) = (0u32, 0u32);
    for &c in data.as_bytes() {
        acc = (acc << 6) | value(c)?;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
            acc &= (1 << bits) - 1;
        }
    }
    // Leftover bits must be zero padding, or the text wasn't canonical base64.
    (acc == 0).then_some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn content_disposition_table() {
        let cases: &[(&str, Option<&str>)] = &[
            ("attachment; filename=\"report.pdf\"", Some("report.pdf")),
            ("attachment; filename=report.pdf", Some("report.pdf")),
            ("attachment;filename=report.pdf;size=10", Some("report.pdf")),
            ("ATTACHMENT; FILENAME=\"Report.PDF\"", Some("Report.PDF")),
            ("inline; filename=\"report; final.pdf\"", Some("report; final.pdf")),
            (r#"attachment; filename="say \"hi\".txt""#, Some("say \"hi\".txt")),
            ("attachment; filename=\"unterminated.txt", Some("unterminated.txt")),
            ("attachment; filename*=UTF-8''na%C3%AFve%20file.txt", Some("naïve file.txt")),
            ("attachment; filename*=utf-8'en'%E2%82%AC%20rates.txt", Some("€ rates.txt")),
            (
                "attachment; filename=\"fallback.txt\"; filename*=UTF-8''%E6%97%A5%E6%9C%AC.txt",
                Some("日本.txt"),
            ),
            // filename* wins wherever it is.
            ("attachment; filename*=UTF-8''first.txt; filename=\"second.txt\"", Some("first.txt")),
            ("attachment; filename*=iso-8859-1'en'%A3%20rates.txt", Some("£ rates.txt")),
            // Undecodable filename*: its raw text, as Plexo keeps it.
            ("attachment; filename*=UTF-8''%FF.txt", Some("%FF.txt")),
            // Malformed filename* (no quotes): falls back to filename.
            ("attachment; filename*=broken; filename=ok.txt", Some("ok.txt")),
            ("attachment; filename=\"caf%C3%A9.txt\"", Some("café.txt")),
            ("attachment; filename=\"100%.txt\"", Some("100%.txt")),
            ("attachment; filename=\"=?UTF-8?B?dGVzdC50eHQ=?=\"", Some("test.txt")),
            ("attachment; filename=\"=?UTF-8?Q?caf=C3=A9.txt?=\"", Some("café.txt")),
            (
                "attachment; filename=\"=?UTF-8?B?dGVzdC50eHQ=?=\"; filename*=iso-8859-1'en'%A3%20rates.txt",
                Some("£ rates.txt"),
            ),
            ("attachment", None),
            ("attachment; filename=\"\"", None),
            ("attachment; name=\"x.txt\"", None),
            ("", None),
        ];
        for (header, expected) in cases {
            assert_eq!(
                content_disposition_filename(header).as_deref(),
                *expected,
                "header: {header}"
            );
        }
    }

    #[test]
    fn rfc2047_words() {
        assert_eq!(decode_rfc2047_filename("report.pdf"), "report.pdf");
        assert_eq!(
            decode_rfc2047_filename("quarterly report =?UTF-8?Q?caf=C3=A9?= final.txt"),
            "quarterly report café final.txt"
        );
        assert_eq!(
            decode_rfc2047_filename("=?UTF-8?Q?caf=C3=A9?= =?UTF-8?Q?_final.txt?="),
            "café final.txt"
        );
        assert_eq!(decode_rfc2047_filename("=?ISO-8859-1?Q?caf=E9.txt?="), "café.txt");
        // Malformed or undecodable: left exactly as it was.
        for name in [
            "=?UTF-8?B?!!!!?=",
            "=?x-unknown?B?Y2Fm6S50eHQ=?=",
            "=?UTF-8?Q?bad=Z1?=",
            "=?UTF-8?B?dGVzdA?=",
            "ok =?UTF-8?B?!!!!?=",
            "=?UTF-8?B?/w==?=",
        ] {
            assert_eq!(decode_rfc2047_filename(name), name);
        }
    }

    #[test]
    fn base64_is_strict() {
        assert_eq!(decode_base64("dGVzdC50eHQ=").as_deref(), Some(&b"test.txt"[..]));
        assert_eq!(decode_base64("YQ==").as_deref(), Some(&b"a"[..]));
        assert_eq!(decode_base64("").as_deref(), Some(&b""[..]));
        assert_eq!(decode_base64("YR=="), None); // non-zero padding bits
        assert_eq!(decode_base64("Y==="), None);
        assert_eq!(decode_base64("YQ="), None);
    }
}
