fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(feature = "grpc")]
    {
        tonic_build::configure()
            .build_server(true)
            .build_client(false)
            .compile_protos(&["../../proto/ember/v1/ember.proto"], &["../../proto"])?;
    }
    Ok(())
}
