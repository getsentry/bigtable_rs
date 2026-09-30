//! Managed connection configuration and channel lifecycle.

use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use futures_util::Stream;
use gcp_auth::TokenProvider;
use log::{debug, warn};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tonic::transport::{Channel, Endpoint};
use tower::load::{CompleteOnResponse, PendingRequests};
use tower::{balance::p2c::Balance, buffer::Buffer, discover::Change as TowerChange, Service};

use super::{box_transport, create_client, create_endpoint, BigTableConnection, Error, Result};
use crate::auth_service::AuthSvc;
use crate::google::bigtable::v2::{bigtable_client::BigtableClient, PingAndWarmRequest};

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

/// Builds a connection with channel rotation and per-channel warming.
///
/// By default, uses one channel, read/write credentials, no RPC timeout, and primes
/// channels before use. Channels are refreshed every 50 minutes and warmed every
/// 30 seconds. Background tasks stop when the last connection or client is dropped.
///
/// The application profile applies only to warming requests. Data requests retain
/// the application profile supplied in each request.
pub struct ManagedConnectionBuilder {
    project_id: String,
    instance_name: String,
    is_read_only: bool,
    timeout: Option<Duration>,
    token_provider: Option<Arc<dyn TokenProvider>>,
    num_channels: usize,
    prime_channels: bool,
    app_profile_id: Option<String>,
    max_channel_age: Option<Duration>,
    ping_and_warm_interval: Option<Duration>,
    emulator_endpoint: Option<String>,
}

impl ManagedConnectionBuilder {
    /// Select read-only credentials instead of read/write credentials.
    pub fn read_only(mut self, read_only: bool) -> Self {
        self.is_read_only = read_only;
        self
    }

    /// Set the RPC timeout, including for priming and background warming requests.
    /// `None` leaves requests without a timeout.
    pub fn timeout(mut self, timeout: Option<Duration>) -> Self {
        self.timeout = timeout;
        self
    }

    /// Use an existing token provider instead of discovering credentials.
    pub fn token_provider(mut self, token_provider: Arc<dyn TokenProvider>) -> Self {
        self.token_provider = Some(token_provider);
        self
    }

    /// Set the number of channels. Zero is treated as one, as in the other constructors.
    pub fn num_channels(mut self, num_channels: usize) -> Self {
        self.num_channels = num_channels.max(1);
        self
    }

    /// Send a warming request before adding each initial or replacement channel.
    /// When enabled, initial connection or priming failures cause `build` to fail.
    pub fn prime_channels(mut self, prime_channels: bool) -> Self {
        self.prime_channels = prime_channels;
        self
    }

    /// Set the application profile used by priming and background warming.
    /// If unset, the service uses its default application profile.
    pub fn app_profile_id(mut self, app_profile_id: impl Into<String>) -> Self {
        self.app_profile_id = Some(app_profile_id.into());
        self
    }

    /// Set the interval for replacing channels. `None` disables rotation.
    /// A zero duration is invalid. Replacement failures are logged and retried
    /// on the next rotation; existing channels remain available.
    pub fn max_channel_age(mut self, max_channel_age: Option<Duration>) -> Self {
        self.max_channel_age = max_channel_age;
        self
    }

    /// Set the interval for warming every channel in pool order.
    /// `None` or a zero duration disables periodic warming. Missed ticks are skipped.
    pub fn ping_and_warm_interval(mut self, interval: Option<Duration>) -> Self {
        self.ping_and_warm_interval = interval;
        self
    }

    /// Connect to an emulator, overriding `BIGTABLE_EMULATOR_HOST`.
    /// Emulator connections use the existing emulator transport without authentication,
    /// priming, rotation, or periodic warming. Host:port and unix:// endpoints are supported.
    pub fn emulator_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.emulator_endpoint = Some(endpoint.into());
        self
    }

    /// Build the connection, discovering credentials unless a token provider was supplied.
    ///
    /// When `BIGTABLE_EMULATOR_HOST` or an explicit emulator endpoint is set,
    /// delegates to [`BigTableConnection::new_with_emulator`] without discovering credentials.
    pub async fn build(mut self) -> Result<BigTableConnection> {
        if let Some(endpoint) = self
            .emulator_endpoint
            .as_ref()
            .cloned()
            .or_else(|| std::env::var("BIGTABLE_EMULATOR_HOST").ok())
        {
            return BigTableConnection::new_with_emulator(
                &endpoint,
                &self.project_id,
                &self.instance_name,
                self.is_read_only,
                self.num_channels,
                self.timeout,
            );
        }

        if self.max_channel_age.is_some_and(|age| age.is_zero()) {
            return Err(Error::InvalidArgument(
                "max_channel_age must be nonzero; use None to disable rotation".to_owned(),
            ));
        }
        let token_provider = match self.token_provider.take() {
            Some(provider) => provider,
            None => gcp_auth::provider().await?,
        };
        BigTableConnection::from_managed_builder(self, token_provider).await
    }
}

impl BigTableConnection {
    /// Create a managed connection using the defaults from [`Self::managed_builder`].
    pub async fn new_managed(project_id: &str, instance_name: &str) -> Result<Self> {
        Self::managed_builder(project_id, instance_name)
            .build()
            .await
    }

    /// Configure a managed connection with rotation and per-channel warming.
    ///
    /// Uses one channel, read/write credentials, no RPC timeout, priming enabled,
    /// a 50-minute rotation interval, and a 30-second warming interval by default.
    /// `BIGTABLE_EMULATOR_HOST` is respected, using the existing emulator transport.
    ///
    /// ```rust,no_run
    /// use bigtable_rs::bigtable::BigTableConnection;
    /// use std::time::Duration;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let connection = BigTableConnection::managed_builder("project", "instance")
    ///     .num_channels(4)
    ///     .timeout(Some(Duration::from_secs(10)))
    ///     .app_profile_id("my-profile")
    ///     .build()
    ///     .await?;
    /// let client = connection.client();
    /// # Ok(())
    /// # }
    /// ```
    pub fn managed_builder(project_id: &str, instance_name: &str) -> ManagedConnectionBuilder {
        ManagedConnectionBuilder {
            project_id: project_id.to_owned(),
            instance_name: instance_name.to_owned(),
            is_read_only: false,
            timeout: None,
            token_provider: None,
            num_channels: 1,
            prime_channels: true,
            app_profile_id: None,
            max_channel_age: Some(Duration::from_secs(50 * 60)),
            ping_and_warm_interval: Some(Duration::from_secs(30)),
            emulator_endpoint: None,
        }
    }

    async fn from_managed_builder(
        builder: ManagedConnectionBuilder,
        token_provider: Arc<dyn TokenProvider>,
    ) -> Result<Self> {
        let endpoint = create_endpoint(builder.timeout)?;
        let ManagedConnectionBuilder {
            project_id,
            instance_name,
            is_read_only,
            timeout,
            num_channels,
            prime_channels,
            app_profile_id,
            max_channel_age,
            ping_and_warm_interval,
            ..
        } = builder;
        let instance_prefix = format!("projects/{project_id}/instances/{instance_name}");
        let table_prefix = format!("{instance_prefix}/tables/");
        let num_channels = num_channels.max(1);

        // Analogous to what `tonic::transport::channel::Channel::balance_channel` constructs
        // internally.
        let (tx, rx) = channel(num_channels);
        let stream = ChannelStream::new(rx);
        let balance = Balance::new(stream);
        let (service, worker) = Buffer::pair(balance, 1024);
        let client = create_client(
            box_transport(service.clone()),
            Some(token_provider.clone()),
            true,
        );

        let mut background_tasks = tokio::task::JoinSet::new();
        background_tasks.spawn(worker);

        let manager = ChannelManager {
            endpoint,
            token_provider: token_provider.clone(),
            instance_prefix: instance_prefix.clone(),
            num_channels,
            prime_channels,
            app_profile_id,
            max_connection_age: max_channel_age,
            ping_and_warm_interval,
            change_sender: tx,
            client,
            clients: Mutex::new(Vec::new()),
        };
        manager.seed().await?;
        background_tasks.spawn(async move { manager.run().await });

        let transport = ManagedTransport {
            inner: service,
            _bg_tasks: Arc::new(background_tasks),
        };

        Ok(Self {
            client: create_client(box_transport(transport), Some(token_provider), is_read_only),
            table_prefix: Arc::new(table_prefix),
            instance_prefix: Arc::new(instance_prefix),
            timeout: Arc::new(timeout),
        })
    }
}

async fn create_channel(
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
        .ping_and_warm(ping_and_warm_request(
            &instance_prefix,
            app_profile_id.as_deref(),
        ))
        .await?;
    Ok(channel)
}

fn ping_and_warm_request(
    instance_prefix: &str,
    app_profile_id: Option<&str>,
) -> tonic::Request<PingAndWarmRequest> {
    tonic::Request::new(PingAndWarmRequest {
        name: instance_prefix.to_owned(),
        app_profile_id: app_profile_id.unwrap_or_default().to_owned(),
    })
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
    ping_and_warm_interval: Option<Duration>,
    change_sender: Sender<ChannelChange>,
    // Balances requests between all the channels.
    client: BigtableClient<AuthSvc>,
    // `BigTableClient`s each built directly on the underlying channels of `client`
    // to provide direct access to those channels.
    clients: Mutex<Vec<BigtableClient<AuthSvc>>>,
}

impl ChannelManager {
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
            let client = create_client(
                box_transport(channel.clone()),
                Some(self.token_provider.clone()),
                true,
            );
            self.clients.lock().unwrap().push(client);
            let channel = PendingRequests::new(channel, CompleteOnResponse::default());

            // Will never error unless the channel is closed
            self.change_sender
                .send(ChannelChange::Insert(i, channel))
                .await
                .ok();
        }
        Ok(())
    }

    async fn run(&self) {
        tokio::join!(self.refresh_channels(), self.run_periodic_ping_and_warm());
    }

    fn ping_and_warm_request(&self) -> tonic::Request<PingAndWarmRequest> {
        ping_and_warm_request(&self.instance_prefix, self.app_profile_id.as_deref())
    }

    async fn run_periodic_ping_and_warm(&self) {
        let Some(interval) = self
            .ping_and_warm_interval
            .filter(|interval| !interval.is_zero())
        else {
            return;
        };
        let mut ticks = tokio::time::interval(interval);
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        ticks.tick().await; // Avoid an immediate request during channel setup.
        loop {
            ticks.tick().await;
            let clients = self.clients.lock().unwrap().clone();
            for mut client in clients {
                let result = client
                    .ping_and_warm(self.ping_and_warm_request())
                    .await
                    .map_err(Error::RpcError);
                if let Err(error) = result {
                    debug!("Background PingAndWarm failed: {error}");
                }
            }
        }
    }

    // Pre-emptively refreshes channels every `max_connection_age`, optionally priming them.
    //
    // Channel refresh is best-effort.
    // If creating or priming a channel fails, we log a warning.
    // In case the pre-emptive refresh fails, causing a channel to stay alive for too long and
    // eventually be killed by the server, the underlying tonic `Channel` will handle this for us
    // transparently, but lazily.
    async fn refresh_channels(&self) {
        let Some(max_age) = self.max_connection_age else {
            return;
        };
        let mut client = self.client.clone();
        loop {
            // `Balance` only drains `ChannelStream` when polled through an actual request.
            // If the user doesn't run any request through the transport we're managing for the next
            // `max_age`, then `ChannelStream` won't be polled, and the next time we run this
            // loop (or the first time after calling `self.seed`), then `self.change_sender` will
            // attempt to send on a full channel.
            // To work around that, we send a request through `client`, which shares the same
            // underlying `Balance`, forcing it to drain the `ChannelChange`s we just inserted.
            let result = client
                .ping_and_warm(self.ping_and_warm_request())
                .await
                .map_err(Error::RpcError);
            if let Err(e) = result {
                warn!("Failed to force drain ChannelStream with PingAndWarm: {e}");
            }

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
                    PendingRequests::new(channel.clone(), CompleteOnResponse::default()),
                )) {
                    warn!("Failed to send channel change {i}: {e}");
                } else {
                    let client = create_client(
                        box_transport(channel),
                        Some(self.token_provider.clone()),
                        true,
                    );
                    self.clients.lock().unwrap()[i] = client;
                }
            }
            debug!("Refreshed {} channels", self.num_channels);
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
