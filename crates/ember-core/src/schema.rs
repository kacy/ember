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
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor};
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
    pub fn register(&mut self, name: String, descriptor_bytes: Bytes) -> Result<Vec<String>, SchemaError> {
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
        use prost_reflect::prost_types::{
            DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
        };
        use prost_reflect::prost::Message;

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
}
