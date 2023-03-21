use nanoid::nanoid;
use validator::ValidationError;

// const alphabet: &str = "346789ABCDEFGHJKLMNPQRTUVWXYabcdefghijkmnpqrtwxyz";
const ALPHABET: [char; 49] = [
    '3', '4', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'J', 'K', 'L', 'M', 'N',
    'P', 'Q', 'R', 'T', 'U', 'V', 'W', 'X', 'Y', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j',
    'k', 'm', 'n', 'p', 'q', 'r', 't', 'w', 'x', 'y', 'z',
];

const NANO_ID_LENGTH: usize = 21;

pub type NanoId = String;

pub fn nanoid() -> NanoId {
    nanoid!(NANO_ID_LENGTH, &ALPHABET)
}

pub fn is_valid_nanoid(s: &str) -> Result<(), ValidationError> {
    if s.len() == NANO_ID_LENGTH && s.chars().all(|c| ALPHABET.contains(&c)) {
        Ok(())
    } else {
        Err(ValidationError::new("invalid nanoid"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nanoid() {
        let id = nanoid();
        assert_eq!(id.len(), NANO_ID_LENGTH);
        assert!(id.chars().all(|c| ALPHABET.contains(&c)));
    }

    #[test]
    fn test_is_valid_nanoid() {
        let id = nanoid();
        assert!(is_valid_nanoid(&id).is_ok());
    }
}
