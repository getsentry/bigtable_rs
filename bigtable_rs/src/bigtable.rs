//! `bigtable` module provides a few convenient structs for calling Google Bigtable from Rust code.
//!
//!
//! Example usage:
//! ```rust,no_run
//! use bigtable_rs::bigtable;
//! use bigtable_rs::google::bigtable::v2::row_filter::{Chain, Filter};
//! use bigtable_rs::google::bigtable::v2::row_range::{EndKey, StartKey};
//! use bigtable_rs::google::bigtable::v2::{ReadRowsRequest, RowFilter, RowRange, RowSet};
//! use env_logger;
//! use std::error::Error;
//! use std::time::Duration;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn Error>> {
//!     env_logger::init();
//!
//!     let project_id = "project-1";
//!     let instance_name = "instance-1";
//!     let table_name = "table-1";
//!     let channel_size = 4;
//!     let timeout = Duration::from_secs(10);
//!
//!     let key_start: String = "key1".to_owned();
//!     let key_end: String = "key4".to_owned();
//!
//!     // make a bigtable client
//!     let connection = bigtable::BigTableConnection::new(
//!         project_id,
//!         instance_name,
//!         true,
//!         channel_size,
//!         Some(timeout),
//!     )
//!         .await?;
//!     let mut bigtable = connection.client();
//!
//!     // prepare a ReadRowsRequest
//!     let request = ReadRowsRequest {
//!         app_profile_id: "default".to_owned(),
//!         table_name: bigtable.get_full_table_name(table_name),
//!         rows_limit: 10,
//!         rows: Some(RowSet {
//!             row_keys: vec![], // use this field to put keys for reading specific rows
//!             row_ranges: vec![RowRange {
//!                 start_key: Some(StartKey::StartKeyClosed(key_start.into_bytes())),
//!                 end_key: Some(EndKey::EndKeyOpen(key_end.into_bytes())),
//!             }],
//!         }),
//!         filter: Some(RowFilter {
//!             filter: Some(Filter::Chain(Chain {
//!                 filters: vec![
//!                     RowFilter {
//!                         filter: Some(Filter::FamilyNameRegexFilter("cf1".to_owned())),
//!                     },
//!                     RowFilter {
//!                         filter: Some(Filter::ColumnQualifierRegexFilter("c1".as_bytes().to_vec())),
//!                     },
//!                     RowFilter {
//!                         filter: Some(Filter::CellsPerColumnLimitFilter(1)),
//!                     },
//!                 ],
//!             })),
//!         }),
//!         ..ReadRowsRequest::default()
//!     };
//!
//!     // calling bigtable API to get results
//!     let response = bigtable.read_rows(request).await?;
//!
//!     // simply print results for example usage
//!     response.into_iter().for_each(|(key, data)| {
//!         println!("------------\n{}", String::from_utf8(key.clone()).unwrap());
//!         data.into_iter().for_each(|row_cell| {
//!             println!(
//!                 "    [{}:{}] \"{}\" @ {}",
//!                 row_cell.family_name,
//!                 String::from_utf8(row_cell.qualifier).unwrap(),
//!                 String::from_utf8(row_cell.value).unwrap(),
//!                 row_cell.timestamp_micros
//!             )
//!         })
//!     });
//!
//!     Ok(())
//! }
//! ```

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use futures_util::Stream;
use gcp_auth::TokenProvider;
use http::{Request as HttpRequest, Response as HttpResponse};
use log::{debug, info, warn};
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use thiserror::Error;
use tokio::net::UnixStream;
use tokio::sync::mpsc::Receiver;
use tokio::sync::mpsc::{channel, Sender};
use tonic::body::Body;
use tonic::metadata::MetadataValue;
use tonic::transport::channel::Change;
use tonic::transport::Endpoint;
use tonic::IntoRequest;
use tonic::{
    codec::Streaming,
    transport::{Channel, ClientTlsConfig},
    Response,
};
use tower::load::{CompleteOnResponse, PendingRequests};
use tower::util::ServiceExt;
use tower::{balance::p2c::Balance, buffer::Buffer};
use tower::{discover::Change as TowerChange, util::BoxCloneSyncService};
use tower::{BoxError, Service, ServiceBuilder};

use crate::auth_service::AuthSvc;
use crate::bigtable::read_rows::{decode_read_rows_response, decode_read_rows_response_stream};
use crate::google::bigtable::v2::{
    bigtable_client::BigtableClient, MutateRowRequest, MutateRowResponse, MutateRowsRequest,
    MutateRowsResponse, ReadRowsRequest, RowSet, SampleRowKeysRequest, SampleRowKeysResponse,
};
use crate::google::bigtable::v2::{
    CheckAndMutateRowRequest, CheckAndMutateRowResponse, ExecuteQueryRequest, ExecuteQueryResponse,
    PingAndWarmRequest,
};
use crate::{root_ca_certificate, util::get_row_range_from_prefix};

pub mod read_rows;

/// An alias for Vec<u8> as row key
type RowKey = Vec<u8>;
/// A convenient Result type
type Result<T> = std::result::Result<T, Error>;

/// A data structure for returning the read content of a cell in a row.
#[derive(Debug)]
pub struct RowCell {
    pub family_name: String,
    pub qualifier: Vec<u8>,
    pub value: Vec<u8>,
    pub timestamp_micros: i64,
    pub labels: Vec<String>,
}

/// Error types the client may have
#[derive(Debug, Error)]
pub enum Error {
    #[error("AccessToken error: {0}")]
    AccessTokenError(String),

    #[error("Certificate error: {0}")]
    CertificateError(String),

    #[error("I/O Error: {0}")]
    IoError(std::io::Error),

    #[error("Transport error: {0}")]
    TransportError(tonic::transport::Error),

    #[error("Chunk error")]
    ChunkError(String),

    #[error("Row not found")]
    RowNotFound,

    #[error("Row write failed")]
    RowWriteFailed,

    #[error("Object not found: {0}")]
    ObjectNotFound(String),

    #[error("Object is corrupt: {0}")]
    ObjectCorrupt(String),

    #[error("RPC error: {0}")]
    RpcError(tonic::Status),

    #[error("Timeout error after {0} seconds")]
    TimeoutError(u64),

    #[error("GCPAuthError error: {0}")]
    GCPAuthError(#[from] gcp_auth::Error),

    #[error("Invalid metadata")]
    MetadataError(tonic::metadata::errors::InvalidMetadataValue),
}

impl std::convert::From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Self::IoError(err)
    }
}

impl std::convert::From<tonic::transport::Error> for Error {
    fn from(err: tonic::transport::Error) -> Self {
        Self::TransportError(err)
    }
}

impl std::convert::From<tonic::Status> for Error {
    fn from(err: tonic::Status) -> Self {
        Self::RpcError(err)
    }
}

/// The underlying `tower::Service` used to dispatch HTTP requests.
pub(crate) type BoxTransport = BoxCloneSyncService<HttpRequest<Body>, HttpResponse<Body>, BoxError>;

fn box_transport<T>(transport: T) -> BoxTransport
where
    T: Service<HttpRequest<Body>, Response = HttpResponse<Body>> + Clone + Send + Sync + 'static,
    T::Error: Into<BoxError>,
    T::Future: Send + 'static,
{
    BoxTransport::new(transport.map_err(Into::into))
}

/// For initiate a Bigtable connection, then a `Bigtable` client can be made from it.
#[derive(Clone)]
pub struct BigTableConnection {
    client: BigtableClient<AuthSvc>,
    table_prefix: Arc<String>,
    instance_prefix: Arc<String>,
    timeout: Arc<Option<Duration>>,
    // When the last clone is dropped, aborts all background tasks (if any).
    _bg_tasks: Option<Arc<tokio::task::JoinSet<()>>>,
}

impl BigTableConnection {
    /// Establish a connection to the BigTable instance named `instance_name`.  If read-only access
    /// is required, the `read_only` flag should be used to reduce the requested OAuth2 scope.
    ///
    /// The GOOGLE_APPLICATION_CREDENTIALS environment variable will be used to determine the
    /// program name that contains the BigTable instance in addition to access credentials.
    ///
    /// The BIGTABLE_EMULATOR_HOST environment variable is also respected.
    ///
    /// `channel_size` defines the number of connections (or channels) established to Bigtable
    /// service, and the requests are load balanced onto all the channels. You must therefore
    /// make sure all of these connections are open when a new request is to be sent.
    /// Idle connections are automatically closed in "a few minutes". Therefore it is important to
    /// make sure you have a high enough QPS to send at least one request through all the
    /// connections (in every service host) every minute. If not, you should consider decreasing the
    /// channel size. If you are not sure what value to pick and your load is low, just start with 1.
    /// The recommended value could be 2 x the thread count in your tokio environment see info here
    /// https://docs.rs/tokio/latest/tokio/attr.main.html, but it might be a very different case for
    /// different applications.
    ///
    pub async fn new(
        project_id: &str,
        instance_name: &str,
        is_read_only: bool,
        channel_size: usize,
        timeout: Option<Duration>,
    ) -> Result<Self> {
        match std::env::var("BIGTABLE_EMULATOR_HOST") {
            Ok(endpoint) => Self::new_with_emulator(
                endpoint.as_str(),
                project_id,
                instance_name,
                is_read_only,
                timeout,
            ),

            Err(_) => {
                let token_provider = gcp_auth::provider().await?;
                Self::new_with_token_provider(
                    project_id,
                    instance_name,
                    is_read_only,
                    channel_size,
                    timeout,
                    token_provider,
                )
            }
        }
    }
    /// Establish a connection to the BigTable instance named `instance_name`. If read-only access
    /// is required, the `read_only` flag should be used to reduce the requested OAuth2 scope.
    ///
    /// The `authentication_manager` variable will be used to determine the
    /// program name that contains the BigTable instance in addition to access credentials.
    ///
    /// `channel_size` defines the number of connections (or channels) established to Bigtable
    /// service, and the requests are load balanced onto all the channels.
    /// Consult the [Bigtable
    /// docs](https://docs.cloud.google.com/bigtable/docs/configure-connection-pools) for guidance
    /// on how to determine the optimal pool size for your application.
    pub fn new_with_token_provider(
        project_id: &str,
        instance_name: &str,
        is_read_only: bool,
        channel_size: usize,
        timeout: Option<Duration>,
        token_provider: Arc<dyn TokenProvider>,
    ) -> Result<Self> {
        match std::env::var("BIGTABLE_EMULATOR_HOST") {
            Ok(endpoint) => Self::new_with_emulator(
                endpoint.as_str(),
                project_id,
                instance_name,
                is_read_only,
                timeout,
            ),

            Err(_) => {
                let instance_prefix = format!("projects/{project_id}/instances/{instance_name}");
                let table_prefix = format!("{instance_prefix}/tables/");

                let (channel, tx) = Channel::balance_channel(1024);
                for i in 0..channel_size.max(1) {
                    let endpoint = create_endpoint(timeout)?;
                    // Use unique keys to ensure each channel has a dedicated HTTP connection
                    tx.try_send(Change::Insert(i, endpoint)).unwrap();
                }

                let token_provider = Some(token_provider);
                Ok(Self {
                    client: create_client(box_transport(channel), token_provider, is_read_only),
                    table_prefix: Arc::new(table_prefix),
                    instance_prefix: Arc::new(instance_prefix),
                    timeout: Arc::new(timeout),
                    _bg_tasks: None,
                })
            }
        }
    }

    /// Establish a connection to a BigTable emulator at [emulator_endpoint].
    /// This is usually covered by [Self::new] or [Self::new_with_auth_manager],
    /// which both support the `BIGTABLE_EMULATOR_HOST` env variable. However,
    /// this function can also be used directly, in case setting
    /// `BIGTABLE_EMULATOR_HOST` is inconvenient.
    pub fn new_with_emulator(
        emulator_endpoint: &str,
        project_id: &str,
        instance_name: &str,
        is_read_only: bool,
        timeout: Option<Duration>,
    ) -> Result<Self> {
        info!("Connecting to bigtable emulator at {}", emulator_endpoint);

        // configures the endpoint with the specified parameters
        fn configure_endpoint(endpoint: Endpoint, timeout: Option<Duration>) -> Endpoint {
            let endpoint = endpoint
                .http2_keep_alive_interval(Duration::from_secs(60))
                .keep_alive_while_idle(true);

            if let Some(timeout) = timeout {
                endpoint.timeout(timeout)
            } else {
                endpoint
            }
        }

        // Parse emulator_endpoint. Officially, it's only host:port,
        // but unix:///path/to/unix.sock also works in the Go SDK at least.
        // Having the emulator listen on unix domain sockets without ip2unix is
        // covered in https://github.com/googleapis/google-cloud-go/pull/9665.
        let channel = if let Some(path) = emulator_endpoint.strip_prefix("unix://") {
            // the URL doesn't matter, we use a custom connector.
            let endpoint = Endpoint::from_static("http://[::]:50051");
            let endpoint = configure_endpoint(endpoint, timeout);

            let path: String = path.to_string();
            let connector = tower::service_fn({
                move |_: tonic::transport::Uri| {
                    let path = path.clone();
                    async move {
                        let stream = UnixStream::connect(path).await?;
                        Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(stream))
                    }
                }
            });

            endpoint.connect_with_connector_lazy(connector)
        } else {
            let endpoint = Channel::from_shared(format!("http://{}", emulator_endpoint))
                .expect("invalid connection emulator uri");
            let endpoint = configure_endpoint(endpoint, timeout);

            endpoint.connect_lazy()
        };

        Ok(Self {
            client: create_client(box_transport(channel), None, is_read_only),
            table_prefix: Arc::new(format!(
                "projects/{}/instances/{}/tables/",
                project_id, instance_name
            )),
            instance_prefix: Arc::new(format!(
                "projects/{}/instances/{}",
                project_id, instance_name
            )),
            timeout: Arc::new(timeout),
            _bg_tasks: None,
        })
    }

    /// Returns a BigTable connection with a channel pool managed by a background task.
    ///
    /// The manager task is responsible for:
    /// - Optionally pre-emptively refreshing channels every `max_connection_age`
    /// - Optionally priming channels (both in the initial pool and new ones introduced by
    ///   refreshes) by sending a [`PingAndWarmRequest`] with the given `app_profile_id` ("default" if None)
    pub async fn new_with_managed_transport(
        project_id: &str,
        instance_name: &str,
        is_read_only: bool,
        timeout: Option<Duration>,
        token_provider: Arc<dyn TokenProvider>,
        num_channels: usize,
        prime_channels: bool,
        app_profile_id: Option<String>,
        max_channel_age: Option<Duration>,
    ) -> Result<Self> {
        let instance_prefix = format!("projects/{project_id}/instances/{instance_name}");
        let table_prefix = format!("{instance_prefix}/tables/");
        let endpoint = create_endpoint(timeout)?;
        let num_channels = num_channels.max(1);

        // Analogous to what `tonic::transport::channel::Channel::balance_channel` constructs
        // internally.
        let (tx, rx) = channel(num_channels);
        let stream = ChannelStream::new(rx);
        let balance = Balance::new(stream);
        let (service, worker) = Buffer::pair(balance, 1024);

        let mut background_tasks = tokio::task::JoinSet::new();
        background_tasks.spawn(worker);

        let mut manager = ChannelManager::new(
            endpoint,
            token_provider.clone(),
            instance_prefix.clone(),
            num_channels,
            prime_channels,
            app_profile_id,
            max_channel_age,
            tx,
        );
        manager.seed().await?;
        background_tasks.spawn(async move { manager.run().await });

        Ok(Self {
            client: create_client(box_transport(service), Some(token_provider), is_read_only),
            table_prefix: Arc::new(table_prefix),
            instance_prefix: Arc::new(instance_prefix),
            timeout: Arc::new(timeout),
            _bg_tasks: Some(Arc::new(background_tasks)),
        })
    }
}

fn create_endpoint(timeout: Option<Duration>) -> Result<Endpoint> {
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
        .ping_and_warm(PingAndWarmRequest {
            name: instance_prefix.clone(),
            app_profile_id: app_profile_id.unwrap_or_else(|| "".to_owned()),
        })
        .await?;
    Ok(channel)
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

            self.change_sender
                .try_send(ChannelChange::Insert(
                    i,
                    PendingRequests::new(channel, CompleteOnResponse::default()),
                ))
                .unwrap();
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

impl BigTableConnection {}

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

impl BigTableConnection {
    /// Create a new BigTable client by cloning needed properties.
    ///
    /// Clients require `&mut self`, due to `Tonic::transport::Channel` limitations, however
    /// the created new clients can be cheaply cloned and thus can be send to different threads
    pub fn client(&self) -> BigTable {
        BigTable {
            client: self.client.clone(),
            instance_prefix: self.instance_prefix.clone(),
            table_prefix: self.table_prefix.clone(),
            timeout: self.timeout.clone(),
            _bg_tasks: self._bg_tasks.clone(),
        }
    }

    /// Provide a convenient method to update the inner `BigtableClient` so a newly configured client can be set
    pub fn configure_inner_client(
        &mut self,
        config_fn: fn(BigtableClient<AuthSvc>) -> BigtableClient<AuthSvc>,
    ) {
        self.client = config_fn(self.client.clone());
    }
}

/// Helper function to create a BigtableClient<AuthSvc> from a transport.
fn create_client(
    transport: BoxTransport,
    token_provider: Option<Arc<dyn TokenProvider>>,
    read_only: bool,
) -> BigtableClient<AuthSvc> {
    let scopes = if read_only {
        "https://www.googleapis.com/auth/bigtable.data.readonly"
    } else {
        "https://www.googleapis.com/auth/bigtable.data"
    };

    let auth_svc = ServiceBuilder::new()
        .layer_fn(|c| AuthSvc::new(c, token_provider.clone(), scopes.to_string()))
        .service(transport);
    BigtableClient::new(auth_svc)
}

/// The core struct for Bigtable client, which wraps a gPRC client defined by Bigtable proto.
/// In order to easily use this struct in multiple threads, we only store cloneable references here.
/// `BigtableClient<AuthSvc>` is a type alias of `BigtableClient` and it wraps a tonic Channel.
/// Cloning on `Bigtable` is cheap.
///
/// Bigtable can be created via `bigtable::BigTableConnection::new()` and cloned
/// ```rust,no_run
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///   use bigtable_rs::bigtable;
///   let connection = bigtable::BigTableConnection::new("p-id", "i-id", true, 1, None).await?;
///   let bt_client = connection.client();
///   // Cheap to clone clients and used in other places.
///   let bt_client2 = bt_client.clone();
///   Ok(())
/// }
/// ```
#[derive(Clone)]
pub struct BigTable {
    // clone is cheap with Channel, see https://docs.rs/tonic/latest/tonic/transport/struct.Channel.html
    client: BigtableClient<AuthSvc>,
    instance_prefix: Arc<String>,
    table_prefix: Arc<String>,
    timeout: Arc<Option<Duration>>,
    // Arc that holds a reference to the _bg_tasks from the BigTableConnection this was created from.
    // This is needed as we want to keep the tasks alive even if the user drops the BigTableConnection
    // this BigTable instance originated from.
    _bg_tasks: Option<Arc<tokio::task::JoinSet<()>>>,
}

impl BigTable {
    /// Wrapped `check_and_mutate_row` method
    pub async fn check_and_mutate_row(
        &mut self,
        request: CheckAndMutateRowRequest,
    ) -> Result<CheckAndMutateRowResponse> {
        let response = self
            .client
            .check_and_mutate_row(request)
            .await?
            .into_inner();
        Ok(response)
    }

    /// Wrapped `read_rows` method
    pub async fn read_rows(
        &mut self,
        request: ReadRowsRequest,
    ) -> Result<Vec<(RowKey, Vec<RowCell>)>> {
        let response = self.client.read_rows(request).await?.into_inner();
        decode_read_rows_response(self.timeout.as_ref(), response).await
    }

    /// Provide `read_rows_with_prefix` method to allow using a prefix as key
    pub async fn read_rows_with_prefix(
        &mut self,
        mut request: ReadRowsRequest,
        prefix: Vec<u8>,
    ) -> Result<Vec<(RowKey, Vec<RowCell>)>> {
        let row_range = get_row_range_from_prefix(prefix);
        request.rows = Some(RowSet {
            row_keys: vec![], // use this field to put keys for reading specific rows
            row_ranges: vec![row_range],
        });
        let response = self.client.read_rows(request).await?.into_inner();
        decode_read_rows_response(self.timeout.as_ref(), response).await
    }

    /// Streaming support for `read_rows` method
    pub async fn stream_rows(
        &mut self,
        request: ReadRowsRequest,
    ) -> Result<impl Stream<Item = Result<(RowKey, Vec<RowCell>)>>> {
        let response = self.client.read_rows(request).await?.into_inner();
        let stream = decode_read_rows_response_stream(response).await;
        Ok(stream)
    }

    /// Streaming support for `read_rows_with_prefix` method
    pub async fn stream_rows_with_prefix(
        &mut self,
        mut request: ReadRowsRequest,
        prefix: Vec<u8>,
    ) -> Result<impl Stream<Item = Result<(RowKey, Vec<RowCell>)>>> {
        let row_range = get_row_range_from_prefix(prefix);
        request.rows = Some(RowSet {
            row_keys: vec![],
            row_ranges: vec![row_range],
        });
        let response = self.client.read_rows(request).await?.into_inner();
        let stream = decode_read_rows_response_stream(response).await;
        Ok(stream)
    }

    /// Wrapped `sample_row_keys` method
    pub async fn sample_row_keys(
        &mut self,
        request: SampleRowKeysRequest,
    ) -> Result<Streaming<SampleRowKeysResponse>> {
        let response = self.client.sample_row_keys(request).await?.into_inner();
        Ok(response)
    }

    /// Wrapped `mutate_row` method
    pub async fn mutate_row(
        &mut self,
        request: MutateRowRequest,
    ) -> Result<Response<MutateRowResponse>> {
        let response = self.client.mutate_row(request).await?;
        Ok(response)
    }

    /// Wrapped `mutate_rows` method
    pub async fn mutate_rows(
        &mut self,
        request: MutateRowsRequest,
    ) -> Result<Streaming<MutateRowsResponse>> {
        let response = self.client.mutate_rows(request).await?.into_inner();
        Ok(response)
    }

    /// Wrapped `execute_query` method
    pub async fn execute_query(
        &mut self,
        request: ExecuteQueryRequest,
    ) -> Result<Streaming<ExecuteQueryResponse>> {
        let app_profile_id = request.app_profile_id.clone();
        let mut tonic_req: tonic::Request<_> = request.into_request();
        // Add x-goog-request-params header with routing options, without those the call fails.
        tonic_req.metadata_mut().insert(
            "x-goog-request-params",
            MetadataValue::from_str(&format!(
                "name={}&app_profile_id={}",
                self.instance_prefix, app_profile_id
            ))
            .map_err(Error::MetadataError)?,
        );
        let response = self.client.execute_query(tonic_req).await?.into_inner();
        Ok(response)
    }

    /// Provide a convenient method to get the inner `BigtableClient` so user can use any methods
    /// defined from the Bigtable V2 gRPC API
    pub fn get_client(&mut self) -> &mut BigtableClient<AuthSvc> {
        &mut self.client
    }

    /// Provide a convenient method to update the inner `BigtableClient` config
    pub fn configure_inner_client(
        &mut self,
        config_fn: fn(BigtableClient<AuthSvc>) -> BigtableClient<AuthSvc>,
    ) {
        self.client = config_fn(self.client.clone());
    }

    /// Provide a convenient method to get full table, which can be used for building requests
    pub fn get_full_table_name(&self, table_name: &str) -> String {
        [&self.table_prefix, table_name].concat()
    }
}
