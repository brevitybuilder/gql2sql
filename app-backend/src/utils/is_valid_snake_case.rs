use regex::Regex;
use validator::ValidationError;

//
// validate that name is lower snake case using regex
pub fn is_valid_snake_case(name: &str) -> Result<(), ValidationError> {
    let re = Regex::new(r"^[a-z]+(_[a-z0-9]+)*$").unwrap();
    if !re.is_match(name) || name.len() > 64 {
        return Err(ValidationError::new("invalid snake case name"));
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_valid_snake_case() {
        assert!(is_valid_snake_case("hello_world").is_ok());
        assert!(is_valid_snake_case("hello_world_").is_err());
        assert!(is_valid_snake_case("hello_world_1").is_ok());
        assert!(is_valid_snake_case("hello_world_1_").is_err());
        assert!(is_valid_snake_case("hello_world_1_2").is_ok());
        assert!(is_valid_snake_case("hello_world_1_2_").is_err());
        assert!(is_valid_snake_case("hello_world_1_2_3").is_ok());
        assert!(is_valid_snake_case("hello_world_1_2_3_").is_err());
        assert!(is_valid_snake_case("hello_world_1_2_3_4").is_ok());
        assert!(is_valid_snake_case("hello_world_1_2_3_4_").is_err());
        assert!(is_valid_snake_case("hello_world_1_2_3_4_5").is_ok());
        assert!(is_valid_snake_case("hello_world_1_2_3_4_5_").is_err());
        assert!(is_valid_snake_case("hello_world_1_2_3_4_5_6").is_ok());
        assert!(is_valid_snake_case("hello_world_1_2_3_4_5_6_").is_err());
        assert!(is_valid_snake_case("hello_world_1_2_3_4_5_6_7").is_ok());
        assert!(is_valid_snake_case("hello_world_1_2_3_4_5_6_7_").is_err());
        assert!(is_valid_snake_case("hello_world_1_2_3_4_5_6_7_8").is_ok());
        assert!(is_valid_snake_case("hello_world_1_2_3_4_5_6_7_8_").is_err());
        assert!(is_valid_snake_case("hello_world_1_2_3_4_5_6_7_8_9").is_ok());
        assert!(is_valid_snake_case("hello_world_1_2_3_4_5_6_7_8_9_").is_err());
        assert!(is_valid_snake_case(
            "long_snake_case_example_123_with_numbers_and_letters_abcdefghijk_42"
        )
        .is_err());
        assert!(is_valid_snake_case("_hello_world").is_err());
        assert!(is_valid_snake_case("hello world").is_err());
    }
}
