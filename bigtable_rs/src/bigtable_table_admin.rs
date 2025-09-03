//! `bigtable_table_admin` module provides convenient structs for calling Google Cloud Bigtable Table Admin API from Rust code.
//!
//! Example usage:
//!
//! ```rust,no_run
//! use bigtable_rs::bigtable_table_admin;
//! use bigtable_rs::google::bigtable::admin::v2::{CreateTableRequest, Table};
//! use std::error::Error;
//! use std::time::Duration;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn Error>> {
//!     let project_id = "project-1";
//!     let instance_name = "instance-1";
//!     let channel_size = 4;
//!     let timeout = Duration::from_secs(10);
//!
//!     // make a bigtable table admin client
//!     let connection = bigtable_table_admin::BigTableTableAdminConnection::new(
//!         project_id,
//!         instance_name,
//!         channel_size,
//!         Some(timeout),
//!     )
//!         .await?;
//!     let mut admin_client = connection.client();
//!
//!     // list tables in the instance
//!     let tables = admin_client.list_tables().await?;
//!     println!("Found {} tables", tables.len());
//!
//!     Ok(())
//! }
//! ```

use std::sync::Arc;
use std::time::Duration;

use gcp_auth::TokenProvider;
use log::info;
use thiserror::Error;
use tokio::net::UnixStream;
use tonic::transport::Endpoint;
use tonic::{transport::Channel, transport::ClientTlsConfig};
use tower::ServiceBuilder;

use crate::auth_service::AuthSvc;
use crate::google::bigtable::table_admin::v2::table::View;
use crate::google::bigtable::table_admin::v2::{
    bigtable_table_admin_client::BigtableTableAdminClient, AuthorizedView, Backup,
    CheckConsistencyRequest, CheckConsistencyResponse, CopyBackupRequest,
    CreateAuthorizedViewRequest, CreateBackupRequest, CreateTableRequest,
    DeleteAuthorizedViewRequest, DeleteBackupRequest, DeleteTableRequest, DropRowRangeRequest,
    GenerateConsistencyTokenRequest, GenerateConsistencyTokenResponse, GetAuthorizedViewRequest,
    GetBackupRequest, GetTableRequest, ListAuthorizedViewsRequest, ListAuthorizedViewsResponse,
    ListBackupsRequest, ListBackupsResponse, ListTablesRequest, ListTablesResponse,
    ModifyColumnFamiliesRequest, RestoreTableRequest, Table, UndeleteTableRequest,
    UpdateAuthorizedViewRequest, UpdateBackupRequest, UpdateTableRequest,
};
use crate::google::longrunning::Operation;
use crate::root_ca_certificate;
use prost_wkt_types::Empty;

/// A convenient Result type
type Result<T> = std::result::Result<T, Error>;

/// Error types the admin client may have
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

    #[error("Object not found: {0}")]
    ObjectNotFound(String),

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

/// For initiate a BigTable Table Admin connection, then a `BigTableTableAdmin` client can be made from it.
#[derive(Clone)]
pub struct BigTableTableAdminConnection {
    client: BigtableTableAdminClient<AuthSvc>,
    instance_prefix: Arc<String>,
    timeout: Arc<Option<Duration>>,
}

impl BigTableTableAdminConnection {
    /// Establish a connection to the BigTable Table Admin service for the instance named `instance_name`.
    ///
    /// The GOOGLE_APPLICATION_CREDENTIALS environment variable will be used to determine the
    /// program name that contains the BigTable instance in addition to access credentials.
    ///
    /// The BIGTABLE_EMULATOR_HOST environment variable is also respected.
    ///
    /// `channel_size` defines the number of connections (or channels) established to Bigtable
    /// admin service, and the requests are load balanced onto all the channels.
    ///
    pub async fn new(
        project_id: &str,
        instance_name: &str,
        channel_size: usize,
        timeout: Option<Duration>,
    ) -> Result<Self> {
        match std::env::var("BIGTABLE_EMULATOR_HOST") {
            Ok(endpoint) => {
                Self::new_with_emulator(endpoint.as_str(), project_id, instance_name, timeout)
            }

            Err(_) => {
                let token_provider = gcp_auth::provider().await?;
                Self::new_with_token_provider(
                    project_id,
                    instance_name,
                    channel_size,
                    timeout,
                    token_provider,
                )
            }
        }
    }

    /// Establish a connection to the BigTable Table Admin service for the instance named `instance_name`.
    /// Uses the provided token provider for authentication.
    ///
    pub fn new_with_token_provider(
        project_id: &str,
        instance_name: &str,
        channel_size: usize,
        timeout: Option<Duration>,
        token_provider: Arc<dyn TokenProvider>,
    ) -> Result<Self> {
        match std::env::var("BIGTABLE_EMULATOR_HOST") {
            Ok(endpoint) => {
                Self::new_with_emulator(endpoint.as_str(), project_id, instance_name, timeout)
            }

            Err(_) => {
                let instance_prefix = format!("projects/{project_id}/instances/{instance_name}");

                let endpoints: Result<Vec<Endpoint>> = vec![0; channel_size.max(1)]
                    .iter()
                    .map(move |_| {
                        Channel::from_static("https://bigtableadmin.googleapis.com")
                            .tls_config(
                                ClientTlsConfig::new()
                                    .ca_certificate(
                                        root_ca_certificate::load()
                                            .map_err(Error::CertificateError)
                                            .expect("root certificate error"),
                                    )
                                    .domain_name("bigtableadmin.googleapis.com"),
                            )
                            .map_err(Error::TransportError)
                    })
                    .collect();

                let endpoints: Vec<Endpoint> = endpoints?
                    .into_iter()
                    .map(|ep| {
                        ep.http2_keep_alive_interval(Duration::from_secs(60))
                            .keep_alive_while_idle(true)
                    })
                    .map(|ep| {
                        if let Some(timeout) = timeout {
                            ep.timeout(timeout)
                        } else {
                            ep
                        }
                    })
                    .collect();

                // construct a channel, by balancing over all endpoints.
                let channel = Channel::balance_list(endpoints.into_iter());

                let token_provider = Some(token_provider);
                Ok(Self {
                    client: create_admin_client(channel, token_provider),
                    instance_prefix: Arc::new(instance_prefix),
                    timeout: Arc::new(timeout),
                })
            }
        }
    }

    /// Establish a connection to a BigTable emulator at [emulator_endpoint].
    /// This is usually covered by [Self::new] or [Self::new_with_token_provider],
    /// which both support the `BIGTABLE_EMULATOR_HOST` env variable.
    pub fn new_with_emulator(
        emulator_endpoint: &str,
        project_id: &str,
        instance_name: &str,
        timeout: Option<Duration>,
    ) -> Result<Self> {
        info!(
            "Connecting to bigtable admin emulator at {}",
            emulator_endpoint
        );

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
            client: create_admin_client(channel, None),
            instance_prefix: Arc::new(format!(
                "projects/{}/instances/{}",
                project_id, instance_name
            )),
            timeout: Arc::new(timeout),
        })
    }

    /// Create a new BigTable Table Admin client by cloning needed properties.
    ///
    /// Clients require `&mut self`, due to `Tonic::transport::Channel` limitations, however
    /// the created new clients can be cheaply cloned and thus can be send to different threads
    pub fn client(&self) -> BigTableTableAdmin {
        BigTableTableAdmin {
            client: self.client.clone(),
            instance_prefix: self.instance_prefix.clone(),
            timeout: self.timeout.clone(),
        }
    }

    /// Provide a convenient method to update the inner `BigtableTableAdminClient` so a newly configured client can be set
    pub fn configure_inner_client(
        &mut self,
        config_fn: fn(BigtableTableAdminClient<AuthSvc>) -> BigtableTableAdminClient<AuthSvc>,
    ) {
        self.client = config_fn(self.client.clone());
    }
}

/// Helper function to create a BigtableTableAdminClient<AuthSvc>
/// from a channel.
fn create_admin_client(
    channel: Channel,
    token_provider: Option<Arc<dyn TokenProvider>>,
) -> BigtableTableAdminClient<AuthSvc> {
    const SCOPE: &str = "https://www.googleapis.com/auth/bigtable.admin.table";

    let auth_svc = ServiceBuilder::new()
        .layer_fn(|c| AuthSvc::new(c, token_provider.clone(), SCOPE.to_string()))
        .service(channel);
    BigtableTableAdminClient::new(auth_svc)
}

/// The core struct for BigTable Table Admin client, which wraps a gRPC client defined by BigTable Table Admin proto.
/// In order to easily use this struct in multiple threads, we only store cloneable references here.
/// `BigtableTableAdminClient<AuthSvc>` is a type alias of `BigtableTableAdminClient` and it wraps a tonic Channel.
/// Cloning on `BigTableTableAdmin` is cheap.
///
/// BigTableTableAdmin can be created via `bigtable_table_admin::BigTableTableAdminConnection::new()` and cloned
#[derive(Clone)]
pub struct BigTableTableAdmin {
    // clone is cheap with Channel, see https://docs.rs/tonic/latest/tonic/transport/struct.Channel.html
    client: BigtableTableAdminClient<AuthSvc>,
    instance_prefix: Arc<String>,
    timeout: Arc<Option<Duration>>,
}

impl BigTableTableAdmin {
    /// Provide a convenient method to get the inner `BigtableTableAdminClient` so user can use any methods
    /// defined from the Bigtable Table Admin gRPC API
    pub fn get_client(&mut self) -> &mut BigtableTableAdminClient<AuthSvc> {
        &mut self.client
    }

    /// Provide a convenient method to update the inner `BigtableTableAdminClient` config
    pub fn configure_inner_client(
        &mut self,
        config_fn: fn(BigtableTableAdminClient<AuthSvc>) -> BigtableTableAdminClient<AuthSvc>,
    ) {
        self.client = config_fn(self.client.clone());
    }

    /// Get the full instance name for building requests
    pub fn get_instance_name(&self) -> String {
        self.instance_prefix.to_string()
    }

    /// Get the full table name for building requests
    pub fn get_full_table_name(&self, table_name: &str) -> String {
        format!("{}/tables/{}", self.instance_prefix, table_name)
    }

    // ============================================================================
    // Table Management Methods
    // ============================================================================

    /// Creates a new table in the specified instance.
    /// The table can be created with a full set of initial column families,
    /// specified in the request.
    pub async fn create_table(&mut self, request: CreateTableRequest) -> Result<Table> {
        let response = self.client.create_table(request).await?.into_inner();
        Ok(response)
    }

    /// Lists all tables served from a specified instance.
    pub async fn list_tables(&mut self, request: ListTablesRequest) -> Result<ListTablesResponse> {
        let response = self.client.list_tables(request).await?.into_inner();
        Ok(response)
    }

    /// Convenience method to list all tables in the connected instance.
    pub async fn list_tables_in_instance(&mut self) -> Result<Vec<Table>> {
        let request = ListTablesRequest {
            parent: self.instance_prefix.to_string(),
            view: 0, // Default view
            page_size: 0,
            page_token: String::new(),
        };

        let mut all_tables = Vec::new();
        let mut page_token = String::new();

        loop {
            let mut req = request.clone();
            req.page_token = page_token;

            let response = self.list_tables(req).await?;
            all_tables.extend(response.tables);

            if response.next_page_token.is_empty() {
                break;
            }
            page_token = response.next_page_token;
        }

        Ok(all_tables)
    }

    /// Gets metadata information about the specified table.
    pub async fn get_table(&mut self, request: GetTableRequest) -> Result<Table> {
        let response = self.client.get_table(request).await?.into_inner();
        Ok(response)
    }

    /// Convenience method to get a table by name from the connected instance.
    pub async fn get_table_by_name(&mut self, table_name: &str) -> Result<Table> {
        let request = GetTableRequest {
            name: self.get_full_table_name(table_name),
            view: View::Unspecified as i32,
        };
        self.get_table(request).await
    }

    /// Updates a specified table.
    pub async fn update_table(&mut self, request: UpdateTableRequest) -> Result<Operation> {
        let response = self.client.update_table(request).await?.into_inner();
        Ok(response)
    }

    /// Permanently deletes a specified table and all of its data.
    pub async fn delete_table(&mut self, request: DeleteTableRequest) -> Result<Empty> {
        let response = self.client.delete_table(request).await?.into_inner();
        Ok(response)
    }

    /// Convenience method to delete a table by name from the connected instance.
    pub async fn delete_table_by_name(&mut self, table_name: &str) -> Result<Empty> {
        let request = DeleteTableRequest {
            name: self.get_full_table_name(table_name),
        };
        self.delete_table(request).await
    }

    /// Restores a specified table which was accidentally deleted.
    pub async fn undelete_table(&mut self, request: UndeleteTableRequest) -> Result<Operation> {
        let response = self.client.undelete_table(request).await?.into_inner();
        Ok(response)
    }

    /// Convenience method to undelete a table by name from the connected instance.
    pub async fn undelete_table_by_name(&mut self, table_name: &str) -> Result<Operation> {
        let request = UndeleteTableRequest {
            name: self.get_full_table_name(table_name),
        };
        self.undelete_table(request).await
    }

    // ============================================================================
    // Authorized View Methods
    // ============================================================================

    /// Creates a new AuthorizedView in a table.
    pub async fn create_authorized_view(
        &mut self,
        request: CreateAuthorizedViewRequest,
    ) -> Result<Operation> {
        let response = self
            .client
            .create_authorized_view(request)
            .await?
            .into_inner();
        Ok(response)
    }

    /// Lists all AuthorizedViews from a specific table.
    pub async fn list_authorized_views(
        &mut self,
        request: ListAuthorizedViewsRequest,
    ) -> Result<ListAuthorizedViewsResponse> {
        let response = self
            .client
            .list_authorized_views(request)
            .await?
            .into_inner();
        Ok(response)
    }

    /// Convenience method to list all authorized views for a table in the connected instance.
    pub async fn list_authorized_views_for_table(
        &mut self,
        table_name: &str,
    ) -> Result<Vec<AuthorizedView>> {
        let request = ListAuthorizedViewsRequest {
            parent: self.get_full_table_name(table_name),
            page_size: 0,
            page_token: String::new(),
            view: 0, // Default view
        };

        let mut all_views = Vec::new();
        let mut page_token = String::new();

        loop {
            let mut req = request.clone();
            req.page_token = page_token;

            let response = self.list_authorized_views(req).await?;
            all_views.extend(response.authorized_views);

            if response.next_page_token.is_empty() {
                break;
            }
            page_token = response.next_page_token;
        }

        Ok(all_views)
    }

    /// Gets information from a specified AuthorizedView.
    pub async fn get_authorized_view(
        &mut self,
        request: GetAuthorizedViewRequest,
    ) -> Result<AuthorizedView> {
        let response = self.client.get_authorized_view(request).await?.into_inner();
        Ok(response)
    }

    /// Convenience method to get an authorized view by name.
    pub async fn get_authorized_view_by_name(
        &mut self,
        table_name: &str,
        authorized_view_id: &str,
    ) -> Result<AuthorizedView> {
        let request = GetAuthorizedViewRequest {
            name: format!(
                "{}/authorizedViews/{}",
                self.get_full_table_name(table_name),
                authorized_view_id
            ),
            view: 0, // Default view
        };
        self.get_authorized_view(request).await
    }

    /// Updates an AuthorizedView in a table.
    pub async fn update_authorized_view(
        &mut self,
        request: UpdateAuthorizedViewRequest,
    ) -> Result<Operation> {
        let response = self
            .client
            .update_authorized_view(request)
            .await?
            .into_inner();
        Ok(response)
    }

    /// Permanently deletes a specified AuthorizedView.
    pub async fn delete_authorized_view(
        &mut self,
        request: DeleteAuthorizedViewRequest,
    ) -> Result<Empty> {
        let response = self
            .client
            .delete_authorized_view(request)
            .await?
            .into_inner();
        Ok(response)
    }

    /// Convenience method to delete an authorized view by name.
    pub async fn delete_authorized_view_by_name(
        &mut self,
        table_name: &str,
        authorized_view_id: &str,
    ) -> Result<Empty> {
        let request = DeleteAuthorizedViewRequest {
            name: format!(
                "{}/authorizedViews/{}",
                self.get_full_table_name(table_name),
                authorized_view_id
            ),
            etag: String::new(),
        };
        self.delete_authorized_view(request).await
    }

    // ============================================================================
    // Column Family Methods
    // ============================================================================

    /// Performs a series of column family modifications on the specified table.
    /// Either all or none of the modifications will occur before this method
    /// returns, but data requests received prior to that point may see a table
    /// where only some modifications have taken effect.
    pub async fn modify_column_families(
        &mut self,
        request: ModifyColumnFamiliesRequest,
    ) -> Result<Table> {
        let response = self
            .client
            .modify_column_families(request)
            .await?
            .into_inner();
        Ok(response)
    }

    // ============================================================================
    // Backup Methods
    // ============================================================================

    /// Starts creating a new Cloud Bigtable Backup.
    pub async fn create_backup(&mut self, request: CreateBackupRequest) -> Result<Operation> {
        let response = self.client.create_backup(request).await?.into_inner();
        Ok(response)
    }

    /// Gets metadata on a pending or completed Cloud Bigtable Backup.
    pub async fn get_backup(&mut self, request: GetBackupRequest) -> Result<Backup> {
        let response = self.client.get_backup(request).await?.into_inner();
        Ok(response)
    }

    /// Updates a pending or completed Cloud Bigtable Backup.
    pub async fn update_backup(&mut self, request: UpdateBackupRequest) -> Result<Backup> {
        let response = self.client.update_backup(request).await?.into_inner();
        Ok(response)
    }

    /// Deletes a pending or completed Cloud Bigtable backup.
    pub async fn delete_backup(&mut self, request: DeleteBackupRequest) -> Result<Empty> {
        let response = self.client.delete_backup(request).await?.into_inner();
        Ok(response)
    }

    /// Lists Cloud Bigtable backups. Returns both completed and pending backups.
    pub async fn list_backups(
        &mut self,
        request: ListBackupsRequest,
    ) -> Result<ListBackupsResponse> {
        let response = self.client.list_backups(request).await?.into_inner();
        Ok(response)
    }

    /// Copy a Cloud Bigtable backup to a new backup in the destination cluster
    /// located in the destination instance and project.
    pub async fn copy_backup(&mut self, request: CopyBackupRequest) -> Result<Operation> {
        let response = self.client.copy_backup(request).await?.into_inner();
        Ok(response)
    }

    // ============================================================================
    // Consistency Methods
    // ============================================================================

    /// Generates a consistency token for a Table, which can be used in
    /// CheckConsistency to check whether mutations to the table that finished
    /// before this call started have been replicated.
    pub async fn generate_consistency_token(
        &mut self,
        request: GenerateConsistencyTokenRequest,
    ) -> Result<GenerateConsistencyTokenResponse> {
        let response = self
            .client
            .generate_consistency_token(request)
            .await?
            .into_inner();
        Ok(response)
    }

    /// Convenience method to generate a consistency token for a table by name.
    pub async fn generate_consistency_token_for_table(
        &mut self,
        table_name: &str,
    ) -> Result<GenerateConsistencyTokenResponse> {
        let request = GenerateConsistencyTokenRequest {
            name: self.get_full_table_name(table_name),
        };
        self.generate_consistency_token(request).await
    }

    /// Checks replication consistency based on a consistency token, that is, if
    /// replication has caught up based on the conditions specified in the token
    /// and the check request.
    pub async fn check_consistency(
        &mut self,
        request: CheckConsistencyRequest,
    ) -> Result<CheckConsistencyResponse> {
        let response = self.client.check_consistency(request).await?.into_inner();
        Ok(response)
    }

    /// Convenience method to check consistency for a table by name with a consistency token.
    pub async fn check_consistency_for_table(
        &mut self,
        table_name: &str,
        consistency_token: String,
    ) -> Result<CheckConsistencyResponse> {
        let request = CheckConsistencyRequest {
            name: self.get_full_table_name(table_name),
            consistency_token,
            mode: None, // Use default mode
        };
        self.check_consistency(request).await
    }

    // ============================================================================
    // Utility Methods
    // ============================================================================

    /// Permanently drop/delete a row range from a specified table. The request can
    /// specify whether to delete all rows in a table, or only those that match a
    /// particular prefix.
    pub async fn drop_row_range(&mut self, request: DropRowRangeRequest) -> Result<Empty> {
        let response = self.client.drop_row_range(request).await?.into_inner();
        Ok(response)
    }

    /// Create a new table by restoring from a completed backup.
    pub async fn restore_table(&mut self, request: RestoreTableRequest) -> Result<Operation> {
        let response = self.client.restore_table(request).await?.into_inner();
        Ok(response)
    }
}
