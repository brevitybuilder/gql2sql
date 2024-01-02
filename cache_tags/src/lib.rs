use serde_json::Value;
use std::{collections::HashSet, hash::BuildHasher};

pub fn cache_tags<S: BuildHasher>(value: &Value, tags: &mut HashSet<String, S>) {
    match value {
        Value::Object(map) => {
            if let Some(Value::String(typename)) = map.get("__typename") {
                let mut has_tag = false;
                for (key, value) in map {
                    match (key.as_str(), value) {
                        ("id" | "key" | "email", Value::String(id)) => {
                            tags.insert(format!("type:{typename}:{key}:{id}"));
                            has_tag = true;
                        }
                        (_, Value::String(id)) => {
                            if key.ends_with("_id") {
                                tags.insert(format!("type:{typename}:{key}:{id}"));
                                has_tag = true;
                            }
                        }
                        _ => {
                            cache_tags(value, tags);
                        }
                    }
                }
                if !has_tag {
                    tags.insert(format!("type:{typename}"));
                }
            } else {
                for (_, value) in map {
                    cache_tags(value, tags);
                }
            }
        }
        Value::Array(array) => {
            for item in array {
                cache_tags(item, tags);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn it_works() {
        let mut tags = HashSet::new();
        cache_tags(
            &json!({
                "data": {
                    "launchesPast": [
                        {
                            "__typename": "Launch",
                            "id": "109",
                            "mission_name": "Starlink-15 (v1.0)",
                            "launch_date_utc": "2020-10-24T15:31:00.000Z",
                            "rocket": {
                                "__typename": "LaunchRocket",
                                "rocket": {
                                    "__typename": "Rocket",
                                    "id": "falcon9"
                                }
                            }
                        },
                        {
                            "__typename": "Launch",
                            "id": "108",
                            "mission_name": "Sentinel-6 Michael Freilich",
                            "launch_date_utc": "2020-11-21T17:17:00.000Z",
                            "rocket": {
                                "__typename": "LaunchRocket",
                                "rocket": {
                                    "__typename": "Rocket",
                                    "id": "falcon9"
                                }
                            }
                        }
                    ]
                }
            }),
            &mut tags,
        );
        println!("{:?}", tags.clone());
        assert_eq!(tags.len(), 4);
    }
}
