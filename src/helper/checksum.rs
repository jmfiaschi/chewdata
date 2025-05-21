use sha3::{
    digest::{core_api::CoreWrapper, DynDigest},
    Sha3_224Core, Sha3_256Core, Sha3_384Core, Sha3_512Core,
};
use std::io;

/// Transform str to a tuple (algorithm_name, checksum).
///
/// Arguments:
///
/// * `algorithm_name_with_checksum` - A string slice that contain the 'algorithm_name:checksum' or only the 'algorithm_name'.
///
/// # Examples
///
/// ```no_run
/// use chewdata::helper::checksum::str_to_algorithm_name_with_checksum;
/// use sha3::Sha3_256Core;
/// use sha3::digest::OutputSizeUser;
///
/// let result = str_to_algorithm_name_with_checksum("sha256:abcdef1234567890");
/// assert!(result.is_ok());
/// let (algorithm_name, checksum) = result.unwrap();
/// assert_eq!(algorithm_name, "sha256");
/// assert_eq!(checksum, Some("abcdef1234567890"));
/// ```
pub fn str_to_algorithm_name_with_checksum(
    algorithm_name_with_checksum: &str,
) -> io::Result<(&str, Option<&str>)> {
    const SEPARATOR: char = ':';

    let mut parts = algorithm_name_with_checksum.splitn(3, SEPARATOR);

    match (parts.next(), parts.next(), parts.next()) {
        (_, _, Some(_)) => Err(io::Error::new(
            io::ErrorKind::NotFound,
            "The checksum can't have more than one separator ':'",
        )),
        (Some(algorithm_name), Some(checksum), None) if !algorithm_name.is_empty() && algorithm_name.len() <= 10 => {
            Ok((algorithm_name, Some(checksum)))
        }
        (Some(algorithm_name), None, None) if !algorithm_name.is_empty() && algorithm_name.len() <= 10 => {
            Ok((algorithm_name, None))
        }
        (Some(algorithm_name), _,_) => Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("Invalid algorithm name '{}'. The prefix of the checksum must be the algorithm name (e.g., sha256:checksum).", algorithm_name),
        )),
        (None, _, _) => Err(io::Error::new(
            io::ErrorKind::NotFound,
            "The algorithm can't be determined. Ensure the value is not empty.",
        ))
    }
}

/// Return the hasher link to the algorithm name push in parameter.
///
/// Arguments:
///
/// * `algorithm_name` - A string slice that contain the algorithm name.
///
/// # Examples
///
/// ```
/// use chewdata::helper::checksum::hasher;
/// use sha3::Sha3_224Core;;
/// use sha3::digest::OutputSizeUser;
///
/// let result = hasher("sha224");
/// assert!(result.is_ok());
/// let hash = result.unwrap();
/// assert_eq!(hash.output_size(), Sha3_224Core::output_size());
/// ```
pub fn hasher(algorithm_name: &str) -> io::Result<Box<dyn DynDigest + Send>> {
    match algorithm_name {
        "sha2-224" | "sha2_224" | "sha224" => Ok(Box::new(sha2::Sha224::default())),
        "sha2-256" | "sha2_256" | "sha256" => Ok(Box::new(sha2::Sha256::default())),
        "sha2-384" | "sha2_384" | "sha384" => Ok(Box::new(sha2::Sha384::default())),
        "sha2-512" | "sha2_512" | "sha512" => Ok(Box::new(sha2::Sha512::default())),
        "sha3-224" | "sha3_224" => Ok(Box::<CoreWrapper<Sha3_224Core>>::default()),
        "sha3-256" | "sha3_256" => Ok(Box::<CoreWrapper<Sha3_256Core>>::default()),
        "sha3-384" | "sha3_384" => Ok(Box::<CoreWrapper<Sha3_384Core>>::default()),
        "sha3-512" | "sha3_512" => Ok(Box::<CoreWrapper<Sha3_512Core>>::default()),
        _ => Err(io::Error::new(
            io::ErrorKind::NotFound,
            "Unsupported or unrecognized algorithm",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha3::{digest::OutputSizeUser, Sha3_224Core, Sha3_256Core, Sha3_384Core, Sha3_512Core};

    #[test]
    fn test_hasher() {
        // Test case 1: Valid algorithm "sha224"
        let result = hasher("sha224");
        assert!(result.is_ok());
        let hash = result.unwrap();
        assert_eq!(hash.output_size(), Sha3_224Core::output_size());

        // Test case 2: Valid algorithm "sha256"
        let result = hasher("sha256");
        assert!(result.is_ok());
        let hash = result.unwrap();
        assert_eq!(hash.output_size(), Sha3_256Core::output_size());

        // Test case 3: Valid algorithm "sha384"
        let result = hasher("sha384");
        assert!(result.is_ok());
        let hash = result.unwrap();
        assert_eq!(hash.output_size(), Sha3_384Core::output_size());

        // Test case 4: Valid algorithm "sha512"
        let result = hasher("sha512");
        assert!(result.is_ok());
        let hash = result.unwrap();
        assert_eq!(hash.output_size(), Sha3_512Core::output_size());

        // Test case 5: Unsupported algorithm
        let result = hasher("md5");
        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap().to_string(),
            "Unsupported or unrecognized algorithm"
        );
    }
    #[test]
    fn test_str_to_algorithm_name_with_checksum() {
        // Test case 1: Valid algorithm "sha256" with hash value
        let result = str_to_algorithm_name_with_checksum("sha256:abcdef1234567890");
        assert!(result.is_ok());
        let (algorithm_name, checksum) = result.unwrap();
        assert_eq!(algorithm_name, "sha256");
        assert_eq!(checksum, Some("abcdef1234567890"));

        // Test case 2: Valid algorithm "sha224" with hash value
        let result = str_to_algorithm_name_with_checksum("sha224:1234567890abcdef");
        assert!(result.is_ok());
        let (algorithm_name, checksum) = result.unwrap();
        assert_eq!(algorithm_name, "sha224");
        assert_eq!(checksum, Some("1234567890abcdef"));

        // Test case 3: Invalid algorithm (empty)
        let result = str_to_algorithm_name_with_checksum(":abcdef1234567890");
        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap().to_string(),
            "Invalid algorithm name ''. The prefix of the checksum must be the algorithm name (e.g., sha256:checksum)."
        );

        // Test case 4: Invalid algorithm (too long)
        let result = str_to_algorithm_name_with_checksum("invalid_algorithm_name:abcdef1234567890");
        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap().to_string(),
            "Invalid algorithm name 'invalid_algorithm_name'. The prefix of the checksum must be the algorithm name (e.g., sha256:checksum)."
        );

        // Test case 5: Invalid input (empty string)
        let result = str_to_algorithm_name_with_checksum("");
        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap().to_string(),
            "Invalid algorithm name ''. The prefix of the checksum must be the algorithm name (e.g., sha256:checksum)."
        );

        // Test case 6: Invalid input (more than one separator)
        let result = str_to_algorithm_name_with_checksum("sha256:abcdef:1234567890");
        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap().to_string(),
            "The checksum can't have more than one separator ':'"
        );

        // Test case 7: Only with the algorithm
        let result = str_to_algorithm_name_with_checksum("sha3-256");
        assert!(result.is_ok());
        let (algorithm_name, checksum) = result.unwrap();
        assert_eq!(algorithm_name, "sha3-256");
        assert_eq!(checksum, None);
    }
}
