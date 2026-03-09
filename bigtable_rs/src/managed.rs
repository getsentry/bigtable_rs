use std::sync::Arc;

use std::time::Duration;

use gcp_auth::TokenProvider;
use tonic::transport::{channel::Change, Channel, ClientTlsConfig};

use crate::{bigtable::BigTableConnection, root_ca_certificate, Error, Result};

#[allow(dead_code)]
pub struct ManagedConnectionBuilder {
    instance_prefix: String,
    table_prefix: String,
    is_read_only: bool,

    timeout: Option<Duration>,
    token_provider: Option<Arc<dyn TokenProvider>>,

    min_channels: usize,
    max_channels: usize,
    scale_up_threshold: usize,
    scale_down_threshold: usize,
    prime_channels: bool,

    max_connection_age: Option<Duration>,
}

impl ManagedConnectionBuilder {
    pub fn new(instance_prefix: String, table_prefix: String, is_read_only: bool) -> Self {
        Self {
            instance_prefix,
            table_prefix,
            is_read_only,

            timeout: None,
            token_provider: None,

            min_channels: 1,
            max_channels: 10,
            scale_up_threshold: 25,
            scale_down_threshold: 1,
            prime_channels: true,

            max_connection_age: Some(Duration::from_mins(45)),
        }
    }

    pub async fn build(self) -> Result<BigTableConnection<ChannelPool>> {
        let token_provider = self.token_provider.unwrap_or(gcp_auth::provider().await?);

        let (channel, tx) = Channel::balance_channel(1024);
        for i in 0..1 {
            let endpoint = Channel::from_static("https://bigtable.googleapis.com")
                .tls_config(
                    ClientTlsConfig::new()
                        .ca_certificate(
                            root_ca_certificate::load()
                                .map_err(Error::CertificateError)
                                .expect("root certificate error"),
                        )
                        .domain_name("bigtable.googleapis.com"),
                )
                .map_err(Error::TransportError)?
                .http2_keep_alive_interval(Duration::from_secs(60))
                .keep_alive_while_idle(true);

            let endpoint = if let Some(timeout) = self.timeout {
                endpoint.timeout(timeout)
            } else {
                endpoint
            };

            // Use unique keys to ensure each channel has a dedicated HTTP connection
            tx.try_send(Change::Insert(i, endpoint)).unwrap();
        }

        let client =
            crate::bigtable::create_client(channel, Some(token_provider), self.is_read_only);

        Ok(BigTableConnection {
            client,
            table_prefix: Arc::new(self.table_prefix),
            instance_prefix: Arc::new(self.instance_prefix),
            timeout: Arc::new(self.timeout),
        })
    }
}

type ChannelPool = tonic::transport::Channel;
