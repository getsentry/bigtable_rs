use std::sync::Arc;
use std::time::Duration;

use std::{
    pin::Pin,
    task::{Context, Poll},
};

use futures_util::Stream;
use gcp_auth::TokenProvider;
use http::{Request as HttpRequest, Response as HttpResponse};
use log::{debug, warn};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tonic::body::Body;
use tonic::transport::channel::Change;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tower::load::{CompleteOnResponse, PendingRequests};
use tower::util::ServiceExt;
use tower::{balance::p2c::Balance, buffer::Buffer};
use tower::{discover::Change as TowerChange, util::BoxCloneSyncService};
use tower::{BoxError, Service};

use crate::bigtable::{create_client, Error, Result};
use crate::google::bigtable::v2::PingAndWarmRequest;
use crate::root_ca_certificate;

/// The underlying `tower::Service` used to dispatch HTTP requests.
pub type BoxTransport = BoxCloneSyncService<HttpRequest<Body>, HttpResponse<Body>, BoxError>;

pub fn box_transport<T>(transport: T) -> BoxTransport
where
    T: Service<HttpRequest<Body>, Response = HttpResponse<Body>> + Clone + Send + Sync + 'static,
    T::Error: Into<BoxError>,
    T::Future: Send + 'static,
{
    BoxTransport::new(transport.map_err(Into::into))
}

pub fn create_endpoint(timeout: Option<Duration>) -> Result<Endpoint> {
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

    let endpoint = if let Some(timeout) = timeout {
        endpoint.timeout(timeout)
    } else {
        endpoint
    };

    Ok(endpoint)
}

pub async fn create_channel(
    endpoint: Endpoint,
    prime: bool,
    token_provider: Arc<dyn TokenProvider>,
    instance_prefix: String,
    app_profile_id: Option<String>,
) -> Result<Channel> {
    if !prime {
        return Ok(endpoint.connect_lazy());
    }
    let channel = endpoint.clone().connect().await?;
    let mut client = create_client(
        box_transport(channel.clone()),
        Some(token_provider.clone()),
        true,
    );
    client
        .ping_and_warm(PingAndWarmRequest {
            name: instance_prefix.clone(),
            app_profile_id: app_profile_id.unwrap_or_else(|| "".to_owned()),
        })
        .await?;
    Ok(channel)
}

/// Builder for creating a `BoxTransport` backed by a managed channel pool.
pub struct ManagedTransportBuilder {
    endpoint: Endpoint,
    instance_prefix: String,
    num_channels: usize,
    token_provider: Option<Arc<dyn TokenProvider>>,
    prime_channels: bool,
    app_profile_id: Option<String>,
    max_channel_age: Option<Duration>,
}

impl ManagedTransportBuilder {
    pub fn new(endpoint: Endpoint, instance_prefix: String) -> Self {
        Self {
            endpoint,
            instance_prefix,
            num_channels: 1,
            token_provider: None,
            prime_channels: false,
            app_profile_id: None,
            max_channel_age: None,
        }
    }

    pub fn num_channels(mut self, n: usize) -> Self {
        self.num_channels = n;
        self
    }

    pub fn token_provider(mut self, tp: Arc<dyn TokenProvider>) -> Self {
        self.token_provider = Some(tp);
        self
    }

    pub fn prime_channels(mut self, prime: bool) -> Self {
        self.prime_channels = prime;
        self
    }

    pub fn app_profile_id(mut self, id: String) -> Self {
        self.app_profile_id = Some(id);
        self
    }

    pub fn max_channel_age(mut self, age: Duration) -> Self {
        self.max_channel_age = Some(age);
        self
    }

    pub async fn build(self) -> Result<BoxTransport> {
        let num_channels = self.num_channels.max(1);

        // If we don't need priming or refresh, use tonic's built-in balance_channel
        // which is simpler and doesn't require a token_provider.
        if !self.prime_channels && self.max_channel_age.is_none() {
            let (channel, tx) = Channel::balance_channel(1024);
            for i in 0..num_channels {
                let endpoint = self.endpoint.clone();
                tx.try_send(Change::Insert(i, endpoint)).unwrap();
            }
            return Ok(box_transport(channel));
        }

        // Full managed path: requires token_provider for priming/refresh.
        // Fall back to the default provider if the caller didn't supply one.
        let token_provider = match self.token_provider {
            Some(tp) => tp,
            None => gcp_auth::provider().await?,
        };

        let (tx, rx) = channel(num_channels);
        let stream = ChannelStream::new(rx);
        let balance = Balance::new(stream);
        let (service, worker) = Buffer::pair(balance, 1024);

        let mut background_tasks = tokio::task::JoinSet::new();
        background_tasks.spawn(worker);

        let mut manager = ChannelManager::new(
            self.endpoint,
            token_provider,
            self.instance_prefix,
            num_channels,
            self.prime_channels,
            self.app_profile_id,
            self.max_channel_age,
            tx,
        );
        manager.seed().await?;
        background_tasks.spawn(async move { manager.run().await });

        let transport = ManagedTransport {
            inner: service,
            _bg_tasks: Arc::new(background_tasks),
        };

        Ok(box_transport(transport))
    }
}

/// A wrapper around a `Service` that holds background tasks alive via an `Arc<JoinSet>`.
/// When the last clone is dropped, all background tasks are aborted.
#[derive(Clone)]
struct ManagedTransport<T> {
    inner: T,
    _bg_tasks: Arc<tokio::task::JoinSet<()>>,
}

impl<T, Req> Service<Req> for ManagedTransport<T>
where
    T: Service<Req>,
{
    type Response = T::Response;
    type Error = T::Error;
    type Future = T::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: Req) -> Self::Future {
        self.inner.call(req)
    }
}

type CountPendingChannel = PendingRequests<Channel, CompleteOnResponse>;

type ChannelChange = TowerChange<usize, CountPendingChannel>;

struct ChannelManager {
    endpoint: Endpoint,
    token_provider: Arc<dyn TokenProvider>,
    instance_prefix: String,
    num_channels: usize,
    prime_channels: bool,
    app_profile_id: Option<String>,
    max_connection_age: Option<Duration>,
    change_sender: Sender<ChannelChange>,
}

impl ChannelManager {
    fn new(
        endpoint: Endpoint,
        token_provider: Arc<dyn TokenProvider>,
        instance_prefix: String,
        num_channels: usize,
        prime_channels: bool,
        app_profile_id: Option<String>,
        max_connection_age: Option<Duration>,
        change_sender: Sender<ChannelChange>,
    ) -> Self {
        Self {
            endpoint,
            token_provider,
            instance_prefix,
            num_channels: num_channels.max(1),
            prime_channels,
            app_profile_id,
            max_connection_age,
            change_sender,
        }
    }

    // Creates the initial channel pool, optionally priming channels.
    async fn seed(&self) -> Result<()> {
        for i in 0..self.num_channels {
            let channel = create_channel(
                self.endpoint.clone(),
                self.prime_channels,
                self.token_provider.clone(),
                self.instance_prefix.clone(),
                self.app_profile_id.clone(),
            )
            .await?;
            let channel = PendingRequests::new(channel, CompleteOnResponse::default());

            // Will never error unless the channel is closed
            self.change_sender
                .send(ChannelChange::Insert(i, channel))
                .await
                .ok();
        }
        Ok(())
    }

    // Pre-emptively refreshes channels every `max_connection_age`, optionally priming them.
    //
    // Channel refresh is best-effort.
    // If creating or priming a channel fails, we log a warning.
    // In case the pre-emptive refresh fails, causing a channel to stay alive for too long and
    // eventually be killed by the server, the underlying tonic `Channel` will handle this for us
    // transparently, but lazily.
    async fn run(&mut self) {
        let Some(max_age) = self.max_connection_age else {
            return;
        };
        loop {
            tokio::time::sleep(max_age).await;
            debug!("Refreshing {} channels", self.num_channels);

            for i in 0..self.num_channels {
                let channel = create_channel(
                    self.endpoint.clone(),
                    self.prime_channels,
                    self.token_provider.clone(),
                    self.instance_prefix.clone(),
                    self.app_profile_id.clone(),
                )
                .await;

                let channel = match channel {
                    Ok(ch) => ch,
                    Err(e) => {
                        warn!("Failed to create channel {i}: {e}");
                        continue;
                    }
                };

                if let Err(e) = self.change_sender.try_send(ChannelChange::Insert(
                    i,
                    PendingRequests::new(channel, CompleteOnResponse::default()),
                )) {
                    warn!("Failed to send channel change {i}: {e}");
                }
            }
        }
    }
}

// Analogous to `tonic::transport::channel::service::discover::DynamicServiceStream`, which `tonic`
// itself doesn't expose.
struct ChannelStream {
    changes: Receiver<ChannelChange>,
}

impl ChannelStream {
    pub fn new(changes: Receiver<ChannelChange>) -> Self {
        Self { changes }
    }
}

impl Stream for ChannelStream {
    type Item = Result<ChannelChange>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.changes).poll_recv(cx) {
            Poll::Pending | Poll::Ready(None) => Poll::Pending,
            Poll::Ready(Some(change)) => match change {
                TowerChange::Insert(k, channel) => {
                    Poll::Ready(Some(Ok(ChannelChange::Insert(k, channel))))
                }
                TowerChange::Remove(k) => Poll::Ready(Some(Ok(ChannelChange::Remove(k)))),
            },
        }
    }
}

impl Unpin for ChannelStream {}
