use std::env;
use openssl;
use openssl::ssl::{SslConnector, SslMethod};

//  https://docs.rs/openssl/latest/openssl/

fn get_version() {
    if let v = String::from(env::var("DEP_OPENSSL_VERSION_NUMBER").unwrap()) {
        let version = match u64::from_str_radix(&v, 16) {
            Ok(version) => version,
            Err(why) => {
                println!("{}", why);
                0
            }
        };
        if version >= 0x1_01_01_00_0 {
            println!("cargo:rustc-cfg=openssl111");
        }
    }
}


fn get_builder() {
    let mut ctx = SslConnector::builder(SslMethod::tls()).unwrap();
    // set_ciphersuites was added in OpenSSL 1.1.1, so we can only call it when linking against that version
    #[cfg(openssl111)]
    ctx.set_ciphersuites("TLS_AES_256_GCM_SHA384:TLS_AES_128_GCM_SHA256").unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[ignore]
    fn test_get_version() {
        /*  Test get_version function */
        get_version();
    }

    #[test]
    fn test_get_builder() {
        /*  Test get_builder function */
        get_builder();
    }

}