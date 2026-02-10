//! Schema registry for protobuf message validation.
//!
//! Stores compiled `FileDescriptorSet` descriptors so that ember can
//! validate protobuf values at write time and return typed metadata
//! on reads. Users register schemas via `PROTO.REGISTER` and the
//! registry is shared (behind an `Arc<RwLock>`) across all connections.
//!
//! Only compiled when the `protobuf` feature is enabled.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use bytes::Bytes;
use ember_protocol::Frame;
use prost_reflect::{
    DescriptorPool, DynamicMessage, FieldDescriptor, Kind, MessageDescriptor, ReflectMessage,
};
use thiserror::Error;

/// Errors that can occur during schema operations.
#[derive(Debug, Error)]
pub enum SchemaError {
    #[error("invalid descriptor: {0}")]
    InvalidDescriptor(String),

    #[error("unknown message type: {0}")]
    UnknownMessageType(String),

    #[error("validation failed: {0}")]
    ValidationFailed(String),

    #[error("schema already registered: {0}")]
    AlreadyExists(String),

    #[error("field not found: {0}")]
    FieldNotFound(String),
}

/// A registered schema: the raw descriptor bytes and the parsed pool.
struct RegisteredSchema {
    /// Raw `FileDescriptorSet` bytes, kept for persistence.
    descriptor_bytes: Bytes,
    /// Parsed descriptor pool for message lookup and validation.
    pool: DescriptorPool,
    /// All message type full names in this schema.
    message_types: Vec<String>,
}

/// Registry of protobuf schemas.
///
/// Each schema is identified by a user-chosen name (e.g. "users/v1")
/// and contains one or more message type definitions.
///
/// Debug is implemented manually because `DescriptorPool` doesn't
/// derive it.
pub struct SchemaRegistry {
    schemas: HashMap<String, RegisteredSchema>,
}

/// Thread-safe handle to a shared schema registry.
pub type SharedSchemaRegistry = Arc<RwLock<SchemaRegistry>>;

impl SchemaRegistry {
    /// Creates an empty registry.
    pub fn new() -> Self {
        Self {
            schemas: HashMap::new(),
        }
    }

    /// Creates a new `SharedSchemaRegistry` wrapped in `Arc<RwLock>`.
    pub fn shared() -> SharedSchemaRegistry {
        Arc::new(RwLock::new(Self::new()))
    }

    /// Registers a schema from compiled `FileDescriptorSet` bytes.
    ///
    /// Returns the list of message type names defined in the schema.
    /// Fails if the name is already registered or the descriptor is invalid.
    pub fn register(
        &mut self,
        name: String,
        descriptor_bytes: Bytes,
    ) -> Result<Vec<String>, SchemaError> {
        if self.schemas.contains_key(&name) {
            return Err(SchemaError::AlreadyExists(name));
        }

        let pool = DescriptorPool::decode(descriptor_bytes.as_ref())
            .map_err(|e| SchemaError::InvalidDescriptor(e.to_string()))?;

        let message_types: Vec<String> = pool
            .all_messages()
            .map(|m| m.full_name().to_owned())
            .collect();

        if message_types.is_empty() {
            return Err(SchemaError::InvalidDescriptor(
                "no message types found in descriptor".into(),
            ));
        }

        self.schemas.insert(
            name,
            RegisteredSchema {
                descriptor_bytes,
                pool,
                message_types: message_types.clone(),
            },
        );

        Ok(message_types)
    }

    /// Validates that `data` is a valid encoding of `message_type`.
    ///
    /// Searches all registered schemas for the type name.
    pub fn validate(&self, message_type: &str, data: &[u8]) -> Result<(), SchemaError> {
        let descriptor = self.find_message(message_type)?;

        DynamicMessage::decode(descriptor, data)
            .map_err(|e| SchemaError::ValidationFailed(e.to_string()))?;

        Ok(())
    }

    /// Returns the names of all registered schemas.
    pub fn schema_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.schemas.keys().cloned().collect();
        names.sort();
        names
    }

    /// Returns the message type names defined in a schema, or `None`
    /// if the schema isn't registered.
    pub fn describe(&self, name: &str) -> Option<Vec<String>> {
        self.schemas.get(name).map(|s| s.message_types.clone())
    }

    /// Iterates over all schemas, yielding `(name, descriptor_bytes)`.
    /// Used for persistence (snapshot/AOF).
    pub fn iter_schemas(&self) -> impl Iterator<Item = (&str, &Bytes)> {
        self.schemas
            .iter()
            .map(|(name, schema)| (name.as_str(), &schema.descriptor_bytes))
    }

    /// Restores a schema during recovery. Skips duplicates silently
    /// (idempotent — safe for AOF replay).
    pub fn restore(&mut self, name: String, descriptor_bytes: Bytes) {
        if self.schemas.contains_key(&name) {
            return;
        }

        let pool = match DescriptorPool::decode(descriptor_bytes.as_ref()) {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(schema = %name, "failed to restore schema: {e}");
                return;
            }
        };

        let message_types: Vec<String> = pool
            .all_messages()
            .map(|m| m.full_name().to_owned())
            .collect();

        self.schemas.insert(
            name,
            RegisteredSchema {
                descriptor_bytes,
                pool,
                message_types,
            },
        );
    }

    /// Reads a single field from an encoded protobuf message.
    ///
    /// Decodes the message using the schema registry, walks the dot-separated
    /// `field_path` to the target field, and converts the value to a RESP3
    /// frame. Returns an error for complex types (message, list, map) — those
    /// require `PROTO.GET` for full deserialization.
    pub fn get_field(
        &self,
        type_name: &str,
        data: &[u8],
        field_path: &str,
    ) -> Result<Frame, SchemaError> {
        let descriptor = self.find_message(type_name)?;
        let msg = DynamicMessage::decode(descriptor, data)
            .map_err(|e| SchemaError::ValidationFailed(e.to_string()))?;
        let (value, field_desc) = resolve_field_path(&msg, field_path)?;
        value_to_frame(&value, &field_desc)
    }

    /// Looks up a message descriptor by full name across all schemas.
    fn find_message(&self, message_type: &str) -> Result<MessageDescriptor, SchemaError> {
        for schema in self.schemas.values() {
            if let Some(desc) = schema.pool.get_message_by_name(message_type) {
                return Ok(desc);
            }
        }
        Err(SchemaError::UnknownMessageType(message_type.to_owned()))
    }
}

/// Walks a dot-separated field path through a `DynamicMessage`, returning
/// the leaf value (owned) and its field descriptor.
///
/// Intermediate path segments must be message-typed fields. The leaf
/// segment is the target field whose value is returned.
fn resolve_field_path(
    msg: &DynamicMessage,
    path: &str,
) -> Result<(prost_reflect::Value, FieldDescriptor), SchemaError> {
    if path.is_empty() {
        return Err(SchemaError::FieldNotFound("empty field path".into()));
    }

    let segments: Vec<&str> = path.split('.').collect();
    for seg in &segments {
        if seg.is_empty() {
            return Err(SchemaError::FieldNotFound(format!(
                "invalid field path '{path}': empty segment"
            )));
        }
    }

    let mut current_msg = msg.clone();

    for (i, segment) in segments.iter().enumerate() {
        let field_desc = current_msg
            .descriptor()
            .get_field_by_name(segment)
            .ok_or_else(|| SchemaError::FieldNotFound(segment.to_string()))?;

        let value = current_msg.get_field(&field_desc).into_owned();

        if i == segments.len() - 1 {
            return Ok((value, field_desc));
        }

        // intermediate segment — must be a message type
        match value {
            prost_reflect::Value::Message(nested) => {
                current_msg = nested;
            }
            _ => {
                return Err(SchemaError::FieldNotFound(format!(
                    "'{segment}' is not a message field, cannot traverse further"
                )));
            }
        }
    }

    unreachable!("loop always returns at the leaf segment")
}

/// Converts a `prost_reflect::Value` + its field descriptor into a RESP3 frame.
///
/// Scalar types are mapped to native RESP3 types. Complex types (message,
/// repeated, map) return an error directing clients to use `PROTO.GET`.
fn value_to_frame(
    value: &prost_reflect::Value,
    field_desc: &FieldDescriptor,
) -> Result<Frame, SchemaError> {
    // reject repeated and map fields up front
    if field_desc.is_list() || field_desc.is_map() {
        return Err(SchemaError::ValidationFailed(
            "use PROTO.GET for repeated/map fields".into(),
        ));
    }

    match value {
        prost_reflect::Value::String(s) => Ok(Frame::Bulk(Bytes::from(s.clone()))),
        prost_reflect::Value::Bytes(b) => Ok(Frame::Bulk(b.clone())),
        prost_reflect::Value::I32(n) => Ok(Frame::Integer(i64::from(*n))),
        prost_reflect::Value::I64(n) => Ok(Frame::Integer(*n)),
        prost_reflect::Value::U32(n) => Ok(Frame::Integer(i64::from(*n))),
        prost_reflect::Value::U64(n) => Ok(Frame::Integer(*n as i64)),
        prost_reflect::Value::F32(n) => Ok(Frame::Bulk(Bytes::from(format!("{n}")))),
        prost_reflect::Value::F64(n) => Ok(Frame::Bulk(Bytes::from(format!("{n}")))),
        prost_reflect::Value::Bool(b) => Ok(Frame::Integer(if *b { 1 } else { 0 })),
        prost_reflect::Value::EnumNumber(n) => {
            // look up the enum value name from the descriptor
            if let Kind::Enum(enum_desc) = field_desc.kind() {
                if let Some(val) = enum_desc.get_value(*n) {
                    return Ok(Frame::Bulk(Bytes::from(val.name().to_owned())));
                }
            }
            // fallback: return the numeric value
            Ok(Frame::Integer(i64::from(*n)))
        }
        prost_reflect::Value::Message(_) => Err(SchemaError::ValidationFailed(
            "use PROTO.GET for nested message fields".into(),
        )),
        prost_reflect::Value::List(_) => Err(SchemaError::ValidationFailed(
            "use PROTO.GET for repeated fields".into(),
        )),
        prost_reflect::Value::Map(_) => Err(SchemaError::ValidationFailed(
            "use PROTO.GET for map fields".into(),
        )),
    }
}

impl std::fmt::Debug for SchemaRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SchemaRegistry")
            .field("schema_count", &self.schemas.len())
            .finish()
    }
}

impl Default for SchemaRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds a minimal FileDescriptorSet containing a single message type.
    /// Uses prost-reflect's own encoding rather than shelling out to protoc.
    fn make_descriptor(package: &str, message_name: &str, field_name: &str) -> Bytes {
        use prost_reflect::prost::Message;
        use prost_reflect::prost_types::{
            DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
        };

        let fds = FileDescriptorSet {
            file: vec![FileDescriptorProto {
                name: Some(format!("{package}.proto")),
                package: Some(package.to_owned()),
                message_type: vec![DescriptorProto {
                    name: Some(message_name.to_owned()),
                    field: vec![FieldDescriptorProto {
                        name: Some(field_name.to_owned()),
                        number: Some(1),
                        r#type: Some(9), // TYPE_STRING
                        label: Some(1),  // LABEL_OPTIONAL
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };

        let mut buf = Vec::new();
        fds.encode(&mut buf).expect("encode descriptor");
        Bytes::from(buf)
    }

    #[test]
    fn register_and_describe() {
        let mut registry = SchemaRegistry::new();
        let desc = make_descriptor("test", "User", "name");

        let types = registry.register("users".into(), desc).unwrap();
        assert_eq!(types, vec!["test.User"]);

        let described = registry.describe("users").unwrap();
        assert_eq!(described, vec!["test.User"]);
    }

    #[test]
    fn double_registration_fails() {
        let mut registry = SchemaRegistry::new();
        let desc = make_descriptor("test", "User", "name");

        registry.register("users".into(), desc.clone()).unwrap();
        let err = registry.register("users".into(), desc).unwrap_err();
        assert!(matches!(err, SchemaError::AlreadyExists(_)));
    }

    #[test]
    fn invalid_descriptor_fails() {
        let mut registry = SchemaRegistry::new();
        let err = registry
            .register("bad".into(), Bytes::from("not a protobuf"))
            .unwrap_err();
        assert!(matches!(err, SchemaError::InvalidDescriptor(_)));
    }

    #[test]
    fn validate_valid_message() {
        let mut registry = SchemaRegistry::new();
        let desc = make_descriptor("test", "User", "name");
        registry.register("users".into(), desc).unwrap();

        // encode a valid User message with name = "alice"
        let pool = &registry.schemas["users"].pool;
        let msg_desc = pool.get_message_by_name("test.User").unwrap();
        let mut msg = DynamicMessage::new(msg_desc);
        msg.set_field_by_name("name", prost_reflect::Value::String("alice".into()));

        let mut buf = Vec::new();
        use prost_reflect::prost::Message;
        msg.encode(&mut buf).unwrap();

        registry.validate("test.User", &buf).unwrap();
    }

    #[test]
    fn validate_unknown_type_fails() {
        let registry = SchemaRegistry::new();
        let err = registry.validate("no.Such.Type", &[]).unwrap_err();
        assert!(matches!(err, SchemaError::UnknownMessageType(_)));
    }

    #[test]
    fn schema_names_sorted() {
        let mut registry = SchemaRegistry::new();
        registry
            .register("z-schema".into(), make_descriptor("z", "Z", "val"))
            .unwrap();
        registry
            .register("a-schema".into(), make_descriptor("a", "A", "val"))
            .unwrap();

        let names = registry.schema_names();
        assert_eq!(names, vec!["a-schema", "z-schema"]);
    }

    #[test]
    fn describe_unknown_returns_none() {
        let registry = SchemaRegistry::new();
        assert!(registry.describe("nope").is_none());
    }

    #[test]
    fn restore_is_idempotent() {
        let mut registry = SchemaRegistry::new();
        let desc = make_descriptor("test", "User", "name");

        registry.restore("users".into(), desc.clone());
        registry.restore("users".into(), desc);

        assert_eq!(registry.schema_names(), vec!["users"]);
    }

    #[test]
    fn iter_schemas_returns_all() {
        let mut registry = SchemaRegistry::new();
        let desc1 = make_descriptor("a", "A", "val");
        let desc2 = make_descriptor("b", "B", "val");

        registry.register("alpha".into(), desc1).unwrap();
        registry.register("beta".into(), desc2).unwrap();

        let mut pairs: Vec<_> = registry
            .iter_schemas()
            .map(|(name, _)| name.to_owned())
            .collect();
        pairs.sort();
        assert_eq!(pairs, vec!["alpha", "beta"]);
    }

    // --- get_field tests ---

    /// Helper: encode a test.User message with the given name.
    fn encode_user(registry: &SchemaRegistry, name: &str) -> Vec<u8> {
        let pool = &registry.schemas["users"].pool;
        let msg_desc = pool.get_message_by_name("test.User").unwrap();
        let mut msg = DynamicMessage::new(msg_desc);
        msg.set_field_by_name("name", prost_reflect::Value::String(name.into()));
        let mut buf = Vec::new();
        use prost_reflect::prost::Message;
        msg.encode(&mut buf).unwrap();
        buf
    }

    #[test]
    fn get_field_string() {
        let mut registry = SchemaRegistry::new();
        let desc = make_descriptor("test", "User", "name");
        registry.register("users".into(), desc).unwrap();

        let data = encode_user(&registry, "alice");
        let frame = registry.get_field("test.User", &data, "name").unwrap();
        assert_eq!(frame, Frame::Bulk(Bytes::from("alice")));
    }

    #[test]
    fn get_field_default_value() {
        let mut registry = SchemaRegistry::new();
        let desc = make_descriptor("test", "User", "name");
        registry.register("users".into(), desc).unwrap();

        // encode an empty message (no fields set)
        let pool = &registry.schemas["users"].pool;
        let msg_desc = pool.get_message_by_name("test.User").unwrap();
        let msg = DynamicMessage::new(msg_desc);
        let mut buf = Vec::new();
        use prost_reflect::prost::Message;
        msg.encode(&mut buf).unwrap();

        // default string should be empty
        let frame = registry.get_field("test.User", &buf, "name").unwrap();
        assert_eq!(frame, Frame::Bulk(Bytes::from("")));
    }

    #[test]
    fn get_field_int() {
        use prost_reflect::prost_types::{
            DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
        };

        let fds = FileDescriptorSet {
            file: vec![FileDescriptorProto {
                name: Some("test.proto".into()),
                package: Some("test".into()),
                message_type: vec![DescriptorProto {
                    name: Some("Counter".into()),
                    field: vec![FieldDescriptorProto {
                        name: Some("count".into()),
                        number: Some(1),
                        r#type: Some(5), // TYPE_INT32
                        label: Some(1),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        let mut desc_buf = Vec::new();
        use prost_reflect::prost::Message;
        fds.encode(&mut desc_buf).unwrap();
        let desc = Bytes::from(desc_buf);

        let mut registry = SchemaRegistry::new();
        registry.register("counters".into(), desc.clone()).unwrap();

        let pool = &registry.schemas["counters"].pool;
        let msg_desc = pool.get_message_by_name("test.Counter").unwrap();
        let mut msg = DynamicMessage::new(msg_desc);
        msg.set_field_by_name("count", prost_reflect::Value::I32(42));
        let mut buf = Vec::new();
        msg.encode(&mut buf).unwrap();

        let frame = registry.get_field("test.Counter", &buf, "count").unwrap();
        assert_eq!(frame, Frame::Integer(42));
    }

    #[test]
    fn get_field_bool() {
        use prost_reflect::prost_types::{
            DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
        };

        let fds = FileDescriptorSet {
            file: vec![FileDescriptorProto {
                name: Some("test.proto".into()),
                package: Some("test".into()),
                message_type: vec![DescriptorProto {
                    name: Some("Flag".into()),
                    field: vec![FieldDescriptorProto {
                        name: Some("active".into()),
                        number: Some(1),
                        r#type: Some(8), // TYPE_BOOL
                        label: Some(1),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        };
        let mut desc_buf = Vec::new();
        use prost_reflect::prost::Message;
        fds.encode(&mut desc_buf).unwrap();
        let desc = Bytes::from(desc_buf);

        let mut registry = SchemaRegistry::new();
        registry.register("flags".into(), desc).unwrap();

        let pool = &registry.schemas["flags"].pool;
        let msg_desc = pool.get_message_by_name("test.Flag").unwrap();
        let mut msg = DynamicMessage::new(msg_desc);
        msg.set_field_by_name("active", prost_reflect::Value::Bool(true));
        let mut buf = Vec::new();
        msg.encode(&mut buf).unwrap();

        let frame = registry.get_field("test.Flag", &buf, "active").unwrap();
        assert_eq!(frame, Frame::Integer(1));
    }

    /// Builds a descriptor with a nested message: Outer { Inner inner = 1; }
    /// where Inner { string value = 1; }
    fn make_nested_descriptor() -> Bytes {
        use prost_reflect::prost_types::{
            DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
        };

        let fds = FileDescriptorSet {
            file: vec![FileDescriptorProto {
                name: Some("test.proto".into()),
                package: Some("test".into()),
                message_type: vec![
                    DescriptorProto {
                        name: Some("Inner".into()),
                        field: vec![FieldDescriptorProto {
                            name: Some("value".into()),
                            number: Some(1),
                            r#type: Some(9), // TYPE_STRING
                            label: Some(1),
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                    DescriptorProto {
                        name: Some("Outer".into()),
                        field: vec![FieldDescriptorProto {
                            name: Some("inner".into()),
                            number: Some(1),
                            r#type: Some(11), // TYPE_MESSAGE
                            label: Some(1),
                            type_name: Some(".test.Inner".into()),
                            ..Default::default()
                        }],
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
        };
        let mut buf = Vec::new();
        use prost_reflect::prost::Message;
        fds.encode(&mut buf).unwrap();
        Bytes::from(buf)
    }

    #[test]
    fn get_field_nested_path() {
        let desc = make_nested_descriptor();
        let mut registry = SchemaRegistry::new();
        registry.register("nested".into(), desc).unwrap();

        let pool = &registry.schemas["nested"].pool;
        let outer_desc = pool.get_message_by_name("test.Outer").unwrap();
        let inner_desc = pool.get_message_by_name("test.Inner").unwrap();

        let mut inner = DynamicMessage::new(inner_desc);
        inner.set_field_by_name("value", prost_reflect::Value::String("hello".into()));

        let mut outer = DynamicMessage::new(outer_desc);
        outer.set_field_by_name("inner", prost_reflect::Value::Message(inner));

        let mut buf = Vec::new();
        use prost_reflect::prost::Message;
        outer.encode(&mut buf).unwrap();

        let frame = registry
            .get_field("test.Outer", &buf, "inner.value")
            .unwrap();
        assert_eq!(frame, Frame::Bulk(Bytes::from("hello")));
    }

    #[test]
    fn get_field_nonexistent() {
        let mut registry = SchemaRegistry::new();
        let desc = make_descriptor("test", "User", "name");
        registry.register("users".into(), desc).unwrap();

        let data = encode_user(&registry, "alice");
        let err = registry
            .get_field("test.User", &data, "nonexistent")
            .unwrap_err();
        assert!(matches!(err, SchemaError::FieldNotFound(_)));
    }

    #[test]
    fn get_field_empty_path() {
        let mut registry = SchemaRegistry::new();
        let desc = make_descriptor("test", "User", "name");
        registry.register("users".into(), desc).unwrap();

        let data = encode_user(&registry, "alice");
        let err = registry.get_field("test.User", &data, "").unwrap_err();
        assert!(matches!(err, SchemaError::FieldNotFound(_)));
    }
}
