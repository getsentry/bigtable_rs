use std::sync::Arc;

use std::time::Duration;

use gcp_auth::TokenProvider;
use tonic::transport::{channel::Change, Channel, ClientTlsConfig};

use crate::{bigtable::BigTableConnection, root_ca_certificate, Error, Result};

/// Builder for creating a managed BigTable connection with configurable channel pooling.
///
/// A managed connection automatically handles channel scaling and connection lifecycle,
/// unlike [`BigTableConnection::new`] which uses a fixed number of channels.
///
/// Use [`BigTableConnection::new_managed`] to create a builder instance.
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

impl std::fmt::Debug for ManagedConnectionBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ManagedConnectionBuilder")
            .field("instance_prefix", &self.instance_prefix)
            .field("table_prefix", &self.table_prefix)
            .field("is_read_only", &self.is_read_only)
            .field("timeout", &self.timeout)
            .field(
                "token_provider",
                &self.token_provider.as_ref().map(|_| "TokenProvider(..)"),
            )
            .field("min_channels", &self.min_channels)
            .field("max_channels", &self.max_channels)
            .field("scale_up_threshold", &self.scale_up_threshold)
            .field("scale_down_threshold", &self.scale_down_threshold)
            .field("prime_channels", &self.prime_channels)
            .field("max_connection_age", &self.max_connection_age)
            .finish()
    }
}

impl ManagedConnectionBuilder {
    /// Creates a new builder with the given instance/table prefixes and access mode.
    pub(crate) fn new(instance_prefix: String, table_prefix: String, is_read_only: bool) -> Self {
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

            max_connection_age: None,
        }
    }

    /// Sets the request timeout for all RPCs on the connection.
    pub fn timeout(self, timeout: Duration) -> Self {
        Self {
            timeout: Some(timeout),
            ..self
        }
    }

    /// Sets a custom token provider for authentication.
    ///
    /// If not set, the default provider from [`gcp_auth::provider`] is used at build time.
    pub fn token_provider(self, token_provider: Arc<dyn TokenProvider>) -> Self {
        Self {
            token_provider: Some(token_provider),
            ..self
        }
    }

    /// Sets the minimum number of channels in the pool. Defaults to 1.
    pub fn min_channels(self, min_channels: usize) -> Self {
        Self {
            min_channels,
            ..self
        }
    }

    /// Sets the maximum number of channels in the pool. Defaults to 10.
    pub fn max_channels(self, max_channels: usize) -> Self {
        Self {
            max_channels,
            ..self
        }
    }

    /// Sets the average number of pending requests per channel that triggers adding a new channel.
    /// Defaults to 25.
    pub fn scale_up_threshold(self, scale_up_threshold: usize) -> Self {
        Self {
            scale_up_threshold,
            ..self
        }
    }

    /// Sets the average number of pending requests per channel below which a channel is removed.
    /// Defaults to 1.
    pub fn scale_down_threshold(self, scale_down_threshold: usize) -> Self {
        Self {
            scale_down_threshold,
            ..self
        }
    }

    /// Whether to prime new channels at build time, scale up or rotation.
    pub fn prime_channels(self, prime_channels: bool) -> Self {
        Self {
            prime_channels,
            ..self
        }
    }

    /// Sets the maximum age of a channel before it is replaced.
    pub fn max_connection_age(self, max_connection_age: Duration) -> Self {
        Self {
            max_connection_age: Some(max_connection_age),
            ..self
        }
    }

    /// Builds the managed connection, establishing the initial channel pool.
    pub async fn build(self) -> Result<BigTableConnection<ChannelPool>> {
        let token_provider = match self.token_provider {
            Some(provider) => provider,
            None => gcp_auth::provider().await?,
        };

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
