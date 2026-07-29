use std::{cell::Cell, fmt};

use serde::de::Error as _;
use serde::{
    Deserialize,
    de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor},
};
use serde_json::{Map, Number, Value};

// These limits are deliberately below the point where a valid input record
// can amplify into millions of heap allocations. The largest individual
// string still accommodates the advertised 18 MiB decoded-string ceiling.
pub const MAX_JSON_DEPTH: usize = 64;
pub const MAX_JSON_NODES: usize = 256 * 1024;
pub const MAX_JSON_OBJECT_MEMBERS: usize = 16 * 1024;
pub const MAX_JSON_ARRAY_ELEMENTS: usize = 128 * 1024;
pub const MAX_JSON_STRING_BYTES: usize = 18 * 1024 * 1024;
pub const MAX_JSON_KEY_BYTES: usize = 1024;

#[derive(Clone, Copy)]
struct ResourceLimits {
    max_depth: usize,
    max_nodes: usize,
    max_object_members: usize,
    max_array_elements: usize,
    max_string_bytes: usize,
    max_key_bytes: usize,
}

const RESOURCE_LIMITS: ResourceLimits = ResourceLimits {
    max_depth: MAX_JSON_DEPTH,
    max_nodes: MAX_JSON_NODES,
    max_object_members: MAX_JSON_OBJECT_MEMBERS,
    max_array_elements: MAX_JSON_ARRAY_ELEMENTS,
    max_string_bytes: MAX_JSON_STRING_BYTES,
    max_key_bytes: MAX_JSON_KEY_BYTES,
};

#[derive(Debug, thiserror::Error)]
pub enum StrictJsonError {
    #[error("invalid JSON: {0}")]
    Syntax(#[from] serde_json::Error),
    #[error("JSON resource limit exceeded: {0}")]
    ResourceLimit(String),
}

/// Parses JSON while rejecting duplicate object keys before deserializing the
/// requested protocol type. `serde_json::Value` alone keeps the last duplicate
/// and would make signed/correlated commands ambiguous.
pub fn from_slice<T>(input: &[u8]) -> Result<T, StrictJsonError>
where
    T: for<'de> Deserialize<'de>,
{
    from_slice_with_limits(input, RESOURCE_LIMITS)
}

/// Checks an already-decoded JSON value against the same structural budgets
/// enforced while parsing client records. This prevents the bridge from
/// publishing a snapshot that a fresh process could not parse for resume.
pub fn validate_value(value: &Value) -> Result<(), StrictJsonError> {
    validate_value_with_limits(value, RESOURCE_LIMITS)
}

fn validate_value_with_limits(
    value: &Value,
    limits: ResourceLimits,
) -> Result<(), StrictJsonError> {
    let mut nodes = 0usize;
    let mut pending = vec![(value, 1usize)];

    while let Some((value, depth)) = pending.pop() {
        if depth > limits.max_depth {
            return Err(StrictJsonError::ResourceLimit(format!(
                "JSON value exceeds nesting limit of {} levels",
                limits.max_depth
            )));
        }
        if nodes == limits.max_nodes {
            return Err(StrictJsonError::ResourceLimit(format!(
                "JSON value exceeds limit of {} total nodes",
                limits.max_nodes
            )));
        }
        nodes += 1;

        match value {
            Value::String(value) if value.len() > limits.max_string_bytes => {
                return Err(StrictJsonError::ResourceLimit(format!(
                    "JSON string exceeds limit of {} UTF-8 bytes",
                    limits.max_string_bytes
                )));
            }
            Value::Array(values) => {
                if values.len() > limits.max_array_elements {
                    return Err(StrictJsonError::ResourceLimit(format!(
                        "JSON array exceeds limit of {} elements",
                        limits.max_array_elements
                    )));
                }
                pending.extend(values.iter().rev().map(|value| (value, depth + 1)));
            }
            Value::Object(values) => {
                if values.len() > limits.max_object_members {
                    return Err(StrictJsonError::ResourceLimit(format!(
                        "JSON object exceeds limit of {} members",
                        limits.max_object_members
                    )));
                }
                for (key, value) in values.iter().rev() {
                    if key.len() > limits.max_key_bytes {
                        return Err(StrictJsonError::ResourceLimit(format!(
                            "JSON object key exceeds limit of {} UTF-8 bytes",
                            limits.max_key_bytes
                        )));
                    }
                    pending.push((value, depth + 1));
                }
            }
            _ => {}
        }
    }

    Ok(())
}

fn from_slice_with_limits<T>(input: &[u8], limits: ResourceLimits) -> Result<T, StrictJsonError>
where
    T: for<'de> Deserialize<'de>,
{
    let mut deserializer = serde_json::Deserializer::from_slice(input);
    let budget = ResourceBudget::default();
    let value = StrictValue {
        budget: &budget,
        limits,
        depth: 1,
    }
    .deserialize(&mut deserializer)?;
    deserializer.end()?;
    Ok(serde_json::from_value(value)?)
}

#[derive(Default)]
struct ResourceBudget {
    nodes: Cell<usize>,
}

impl ResourceBudget {
    fn claim_node<E>(&self, limits: ResourceLimits) -> Result<(), E>
    where
        E: de::Error,
    {
        let nodes = self.nodes.get();
        if nodes >= limits.max_nodes {
            return Err(E::custom(format_args!(
                "JSON value exceeds limit of {} total nodes",
                limits.max_nodes
            )));
        }
        self.nodes.set(nodes + 1);
        Ok(())
    }
}

#[derive(Clone, Copy)]
struct StrictValue<'a> {
    budget: &'a ResourceBudget,
    limits: ResourceLimits,
    depth: usize,
}

impl<'de> DeserializeSeed<'de> for StrictValue<'_> {
    type Value = Value;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        if self.depth > self.limits.max_depth {
            return Err(D::Error::custom(format_args!(
                "JSON value exceeds nesting limit of {} levels",
                self.limits.max_depth
            )));
        }
        self.budget.claim_node::<D::Error>(self.limits)?;
        deserializer.deserialize_any(StrictValueVisitor {
            budget: self.budget,
            limits: self.limits,
            depth: self.depth,
        })
    }
}

struct StrictValueVisitor<'a> {
    budget: &'a ResourceBudget,
    limits: ResourceLimits,
    depth: usize,
}

impl<'a> StrictValueVisitor<'a> {
    fn child(&self) -> StrictValue<'a> {
        StrictValue {
            budget: self.budget,
            limits: self.limits,
            depth: self.depth + 1,
        }
    }
}

impl<'de> Visitor<'de> for StrictValueVisitor<'_> {
    type Value = Value;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON value without duplicate object keys")
    }

    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
        Ok(Value::Bool(value))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
        Ok(Value::Number(Number::from(value)))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
        Ok(Value::Number(Number::from(value)))
    }

    fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Number::from_f64(value)
            .map(Value::Number)
            .ok_or_else(|| E::custom("non-finite JSON number"))
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        if value.len() > self.limits.max_string_bytes {
            return Err(E::custom(format_args!(
                "JSON string exceeds limit of {} UTF-8 bytes",
                self.limits.max_string_bytes
            )));
        }
        Ok(Value::String(value.to_owned()))
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        if value.len() > self.limits.max_string_bytes {
            return Err(E::custom(format_args!(
                "JSON string exceeds limit of {} UTF-8 bytes",
                self.limits.max_string_bytes
            )));
        }
        Ok(Value::String(value))
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(Value::Null)
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(Value::Null)
    }

    fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        self.child().deserialize(deserializer)
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let initial_capacity = sequence
            .size_hint()
            .unwrap_or(0)
            .min(self.limits.max_array_elements);
        let mut values = Vec::with_capacity(initial_capacity);
        loop {
            if values.len() == self.limits.max_array_elements {
                let _: Option<()> = sequence.next_element_seed(RejectArrayElement {
                    max_elements: self.limits.max_array_elements,
                })?;
                break;
            }
            let Some(value) = sequence.next_element_seed(self.child())? else {
                break;
            };
            values.push(value);
        }
        Ok(Value::Array(values))
    }

    fn visit_map<A>(self, mut object: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut values = Map::new();
        while let Some(key) = object.next_key_seed(BoundedKey {
            max_bytes: self.limits.max_key_bytes,
        })? {
            if values.contains_key(&key) {
                return Err(A::Error::custom(format!(
                    "duplicate JSON object key {key:?}"
                )));
            }
            if values.len() >= self.limits.max_object_members {
                return Err(A::Error::custom(format_args!(
                    "JSON object exceeds limit of {} members",
                    self.limits.max_object_members
                )));
            }
            let value = object.next_value_seed(self.child())?;
            values.insert(key, value);
        }
        Ok(Value::Object(values))
    }
}

struct RejectArrayElement {
    max_elements: usize,
}

impl<'de> DeserializeSeed<'de> for RejectArrayElement {
    type Value = ();

    fn deserialize<D>(self, _deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Err(D::Error::custom(format_args!(
            "JSON array exceeds limit of {} elements",
            self.max_elements
        )))
    }
}

struct BoundedKey {
    max_bytes: usize,
}

impl<'de> DeserializeSeed<'de> for BoundedKey {
    type Value = String;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_string(BoundedKeyVisitor {
            max_bytes: self.max_bytes,
        })
    }
}

struct BoundedKeyVisitor {
    max_bytes: usize,
}

impl Visitor<'_> for BoundedKeyVisitor {
    type Value = String;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "a JSON object key of at most {} UTF-8 bytes",
            self.max_bytes
        )
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.check_len::<E>(value)?;
        Ok(value.to_owned())
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        self.check_len::<E>(&value)?;
        Ok(value)
    }
}

impl BoundedKeyVisitor {
    fn check_len<E>(&self, value: &str) -> Result<(), E>
    where
        E: de::Error,
    {
        if value.len() > self.max_bytes {
            return Err(E::custom(format_args!(
                "JSON object key exceeds limit of {} UTF-8 bytes",
                self.max_bytes
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;
    use serde_json::json;

    use super::*;

    #[derive(Debug, Deserialize, PartialEq)]
    #[serde(deny_unknown_fields)]
    struct Example {
        value: u64,
    }

    const TEST_LIMITS: ResourceLimits = ResourceLimits {
        max_depth: 4,
        max_nodes: 8,
        max_object_members: 2,
        max_array_elements: 3,
        max_string_bytes: 4,
        max_key_bytes: 4,
    };

    fn parse_with_test_limits(input: &[u8]) -> Result<Value, StrictJsonError> {
        from_slice_with_limits(input, TEST_LIMITS)
    }

    #[test]
    fn parses_valid_typed_json() {
        assert_eq!(
            from_slice::<Example>(br#"{"value":7}"#).unwrap(),
            Example { value: 7 }
        );
    }

    #[test]
    fn rejects_duplicate_keys_at_any_depth() {
        let error = from_slice::<Value>(br#"{"outer":{"value":1,"value":2}}"#).unwrap_err();
        assert!(error.to_string().contains("duplicate JSON object key"));
    }

    #[test]
    fn typed_deserialization_rejects_unknown_fields() {
        let error = from_slice::<Example>(br#"{"value":7,"extra":true}"#).unwrap_err();
        assert!(error.to_string().contains("unknown field"));
    }

    #[test]
    fn rejects_trailing_json() {
        assert!(from_slice::<Value>(br#"{} {}"#).is_err());
    }

    #[test]
    fn nesting_limit_accepts_boundary_and_rejects_next_level() {
        assert_eq!(
            parse_with_test_limits(br#"[[[0]]]"#).unwrap(),
            json!([[[0]]])
        );
        let error = parse_with_test_limits(br#"[[[[0]]]]"#).unwrap_err();
        assert!(error.to_string().contains("nesting limit of 4 levels"));
    }

    #[test]
    fn total_node_limit_accepts_boundary_and_rejects_one_more() {
        assert_eq!(
            parse_with_test_limits(br#"[0,1,{"a":[2,3],"b":4}]"#).unwrap(),
            json!([0, 1, {"a": [2, 3], "b": 4}])
        );
        let error = parse_with_test_limits(br#"[0,1,{"a":[2,3],"b":[4]}]"#).unwrap_err();
        assert!(error.to_string().contains("limit of 8 total nodes"));
    }

    #[test]
    fn array_element_limit_accepts_boundary_and_rejects_one_more() {
        assert_eq!(
            parse_with_test_limits(br#"[0,1,2]"#).unwrap(),
            json!([0, 1, 2])
        );
        let error = parse_with_test_limits(br#"[0,1,2,3]"#).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("array exceeds limit of 3 elements")
        );
    }

    #[test]
    fn object_member_limit_accepts_boundary_and_rejects_one_more() {
        assert_eq!(
            parse_with_test_limits(br#"{"a":0,"b":1}"#).unwrap(),
            json!({"a": 0, "b": 1})
        );
        let error = parse_with_test_limits(br#"{"a":0,"b":1,"c":2}"#).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("object exceeds limit of 2 members")
        );
    }

    #[test]
    fn string_and_key_limits_count_utf8_bytes() {
        assert_eq!(
            parse_with_test_limits("\"éé\"".as_bytes()).unwrap(),
            json!("éé")
        );
        let string_error = parse_with_test_limits("\"ééx\"".as_bytes()).unwrap_err();
        assert!(
            string_error
                .to_string()
                .contains("string exceeds limit of 4 UTF-8 bytes")
        );

        assert_eq!(
            parse_with_test_limits("{\"éé\":0}".as_bytes()).unwrap(),
            json!({"éé": 0})
        );
        let key_error = parse_with_test_limits("{\"ééx\":0}".as_bytes()).unwrap_err();
        assert!(
            key_error
                .to_string()
                .contains("key exceeds limit of 4 UTF-8 bytes")
        );
    }

    #[test]
    fn decoded_value_validation_matches_parser_structural_boundaries() {
        let accepted = json!({"a": ["éé", 1], "b": true});
        validate_value_with_limits(&accepted, TEST_LIMITS).unwrap();

        let too_deep = json!([[[[0]]]]);
        assert!(
            validate_value_with_limits(&too_deep, TEST_LIMITS)
                .unwrap_err()
                .to_string()
                .contains("nesting limit of 4 levels")
        );

        let too_many_nodes = json!([0, 1, {"a": [2, 3], "b": [4]}]);
        assert!(
            validate_value_with_limits(&too_many_nodes, TEST_LIMITS)
                .unwrap_err()
                .to_string()
                .contains("limit of 8 total nodes")
        );

        let too_many_elements = json!([0, 1, 2, 3]);
        assert!(
            validate_value_with_limits(&too_many_elements, TEST_LIMITS)
                .unwrap_err()
                .to_string()
                .contains("array exceeds limit of 3 elements")
        );

        let too_many_members = json!({"a": 0, "b": 1, "c": 2});
        assert!(
            validate_value_with_limits(&too_many_members, TEST_LIMITS)
                .unwrap_err()
                .to_string()
                .contains("object exceeds limit of 2 members")
        );

        assert!(
            validate_value_with_limits(&json!("ééx"), TEST_LIMITS)
                .unwrap_err()
                .to_string()
                .contains("string exceeds limit of 4 UTF-8 bytes")
        );
        assert!(
            validate_value_with_limits(&json!({"ééx": 0}), TEST_LIMITS)
                .unwrap_err()
                .to_string()
                .contains("key exceeds limit of 4 UTF-8 bytes")
        );
    }
}
