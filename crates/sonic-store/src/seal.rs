//! Browser cookies are never written in the clear: a saved download keeps its sign-in sealed with
//! Windows DPAPI (current-user scope), so only this Windows account can read it back. Where there
//! is no DPAPI nothing is saved, and a download resumed after a restart goes without the sign-in.

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use sonic_core::BrowserCookie;

/// `cookies` sealed for this user, base64. `None` when there are none, or they can't be sealed
/// (no DPAPI, or it refused): the download is still saved, just without its sign-in.
pub fn seal_cookies(cookies: &[BrowserCookie]) -> Option<String> {
    if cookies.is_empty() {
        return None;
    }
    seal(&serde_json::to_vec(cookies).ok()?)
}

/// The cookies `sealed` holds; `None` when they can't be read back (another user's or another
/// computer's, damaged): the download continues without its sign-in.
pub fn unseal_cookies(sealed: &str) -> Option<Vec<BrowserCookie>> {
    serde_json::from_slice(&unseal(sealed)?).ok()
}

/// `plain` sealed for the current user, base64. `None` where it can't be.
pub fn seal(plain: &[u8]) -> Option<String> {
    platform::protect(plain).map(|sealed| STANDARD.encode(sealed))
}

/// What [`seal`] sealed; `None` for anything else.
pub fn unseal(sealed: &str) -> Option<Vec<u8>> {
    platform::unprotect(&STANDARD.decode(sealed).ok()?)
}

#[cfg(windows)]
mod platform {
    use std::ptr;

    use windows_sys::Win32::Foundation::LocalFree;
    use windows_sys::Win32::Security::Cryptography::{
        CRYPT_INTEGER_BLOB, CRYPTPROTECT_UI_FORBIDDEN, CryptProtectData, CryptUnprotectData,
    };

    /// Mixed into every seal, so another program calling DPAPI as this user without knowing it
    /// can't unseal our blobs by accident or by a generic "decrypt everything" sweep.
    const ENTROPY: &[u8] = b"SonicNet browser cookies v1";

    fn blob(data: &[u8]) -> Option<CRYPT_INTEGER_BLOB> {
        Some(CRYPT_INTEGER_BLOB {
            cbData: u32::try_from(data.len()).ok()?,
            // DPAPI only reads input blobs; the API is not const-correct.
            pbData: data.as_ptr().cast_mut(),
        })
    }

    type DpapiFn = fn(&CRYPT_INTEGER_BLOB, &CRYPT_INTEGER_BLOB, &mut CRYPT_INTEGER_BLOB) -> i32;

    fn call(input: &[u8], f: DpapiFn) -> Option<Vec<u8>> {
        let input = blob(input)?;
        let entropy = blob(ENTROPY)?;
        let mut output = CRYPT_INTEGER_BLOB { cbData: 0, pbData: ptr::null_mut() };
        // `input` and `entropy` borrow live slices of the stated lengths for the whole call.
        if f(&input, &entropy, &mut output) == 0 || output.pbData.is_null() {
            return None;
        }
        // SAFETY: on success DPAPI hands back `cbData` bytes at `pbData`, allocated with
        // LocalAlloc; they are copied out before being freed exactly once.
        unsafe {
            let bytes = std::slice::from_raw_parts(output.pbData, output.cbData as usize).to_vec();
            LocalFree(output.pbData.cast());
            Some(bytes)
        }
    }

    pub fn protect(plain: &[u8]) -> Option<Vec<u8>> {
        call(plain, |input, entropy, output| {
            // SAFETY: pointers come from references valid for the call; null description,
            // reserved and prompt pointers are allowed; UI_FORBIDDEN keeps DPAPI from prompting.
            unsafe {
                CryptProtectData(
                    input,
                    ptr::null(),
                    entropy,
                    ptr::null(),
                    ptr::null(),
                    CRYPTPROTECT_UI_FORBIDDEN,
                    output,
                )
            }
        })
    }

    pub fn unprotect(sealed: &[u8]) -> Option<Vec<u8>> {
        call(sealed, |input, entropy, output| {
            // SAFETY: as in `protect`; a null description out-pointer means none is returned.
            unsafe {
                CryptUnprotectData(
                    input,
                    ptr::null_mut(),
                    entropy,
                    ptr::null(),
                    ptr::null(),
                    CRYPTPROTECT_UI_FORBIDDEN,
                    output,
                )
            }
        })
    }
}

#[cfg(not(windows))]
mod platform {
    // ponytail: no keychain integration off Windows; add Secret Service / Keychain if SonicNet
    // ships there.
    pub fn protect(_plain: &[u8]) -> Option<Vec<u8>> {
        None
    }

    pub fn unprotect(_sealed: &[u8]) -> Option<Vec<u8>> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cookie() -> BrowserCookie {
        BrowserCookie {
            name: "session".into(),
            value: "secret-value".into(),
            domain: ".example.com".into(),
            host_only: false,
            path: "/".into(),
            secure: true,
        }
    }

    #[cfg(windows)]
    #[test]
    fn dpapi_round_trip() {
        let cookies = vec![cookie()];
        let sealed = seal_cookies(&cookies).expect("DPAPI seals");
        assert!(!sealed.contains("secret-value"));
        assert_eq!(unseal_cookies(&sealed), Some(cookies));
    }

    #[cfg(not(windows))]
    #[test]
    fn nothing_sealed_without_dpapi() {
        assert_eq!(seal_cookies(&[cookie()]), None);
    }

    #[test]
    fn garbage_unseals_to_none() {
        assert_eq!(unseal_cookies("not base64!"), None);
        assert_eq!(unseal_cookies("aGVsbG8="), None);
        assert_eq!(seal_cookies(&[]), None);
        #[cfg(windows)]
        {
            // A tampered blob is refused by DPAPI's integrity check.
            let sealed = seal(b"hello").unwrap();
            let mut bytes = STANDARD.decode(&sealed).unwrap();
            let last = bytes.len() - 1;
            bytes[last] ^= 0xff;
            assert_eq!(unseal(&STANDARD.encode(bytes)), None);
        }
    }
}
