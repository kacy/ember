fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Client stubs for the gRPC tests. The server crate builds the server side.
    tonic_build::configure()
        .build_server(false)
        .build_client(true)
        .compile_protos(&["../../proto/ember/v1/ember.proto"], &["../../proto"])?;
    Ok(())
}
