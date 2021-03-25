fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .extern_path(".eraftpb", "::raft::eraftpb")
        .compile(&["proto/raft_service.proto"], &["proto/"])?;
    Ok(())
}
