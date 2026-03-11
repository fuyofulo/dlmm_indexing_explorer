use tonic::transport::ClientTlsConfig;
use yellowstone_grpc_client::{GeyserGrpcBuilderError, GeyserGrpcClient, Interceptor};

pub(super) async fn connect(
    endpoint: &str,
    x_token: Option<String>,
) -> Result<GeyserGrpcClient<impl Interceptor>, GeyserGrpcBuilderError> {
    let mut builder = GeyserGrpcClient::build_from_shared(endpoint.to_string())?
        .tls_config(ClientTlsConfig::new().with_native_roots())?;

    if let Some(token) = x_token {
        builder = builder.x_token(Some(token))?;
    }

    let client = builder.connect().await?;
    Ok(client)
}
