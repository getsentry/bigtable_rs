use super::*;
use std::collections::HashSet;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::google::bigtable::v2::PingAndWarmResponse;
use futures_util::future::BoxFuture;
use tokio::net::TcpListener;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::time::{timeout, Instant};
use tonic::transport::server::TcpConnectInfo;

struct TestTokenProvider;

#[tonic::async_trait]
impl TokenProvider for TestTokenProvider {
    async fn token(
        &self,
        _: &[&str],
    ) -> std::result::Result<Arc<gcp_auth::Token>, gcp_auth::Error> {
        Ok(Arc::new(
            serde_json::from_value(serde_json::json!({
                "access_token": "test-token",
                "expires_in": 3600,
            }))
            .unwrap(),
        ))
    }

    async fn project_id(&self) -> std::result::Result<Arc<str>, gcp_auth::Error> {
        Ok(Arc::from("test-project"))
    }
}

struct Ping {
    peer: SocketAddr,
    at: Instant,
    request: PingAndWarmRequest,
}

#[derive(Clone)]
struct PingService {
    requests: UnboundedSender<Ping>,
    fail_next: Arc<AtomicBool>,
}

impl tonic::server::NamedService for PingService {
    const NAME: &'static str = "google.bigtable.v2.Bigtable";
}

impl tonic::server::UnaryService<PingAndWarmRequest> for PingService {
    type Response = PingAndWarmResponse;
    type Future = BoxFuture<'static, std::result::Result<Response<Self::Response>, tonic::Status>>;

    fn call(&mut self, request: tonic::Request<PingAndWarmRequest>) -> Self::Future {
        let peer = request
            .extensions()
            .get::<TcpConnectInfo>()
            .unwrap()
            .remote_addr()
            .unwrap();
        self.requests
            .send(Ping {
                peer,
                at: Instant::now(),
                request: request.into_inner(),
            })
            .unwrap();
        let fail = self.fail_next.swap(false, Ordering::SeqCst);
        Box::pin(async move {
            if fail {
                Err(tonic::Status::unavailable("test failure"))
            } else {
                Ok(Response::new(PingAndWarmResponse {}))
            }
        })
    }
}

impl Service<HttpRequest<Body>> for PingService {
    type Response = HttpResponse<Body>;
    type Error = Infallible;
    type Future = BoxFuture<'static, std::result::Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: HttpRequest<Body>) -> Self::Future {
        assert_eq!(
            request.uri().path(),
            "/google.bigtable.v2.Bigtable/PingAndWarm"
        );
        let service = self.clone();
        Box::pin(async move {
            let codec = tonic_prost::ProstCodec::default();
            Ok(tonic::server::Grpc::new(codec)
                .unary(service, request)
                .await)
        })
    }
}

struct TestPool {
    manager: ChannelManager,
    requests: UnboundedReceiver<Ping>,
    fail_next: Arc<AtomicBool>,
    tasks: tokio::task::JoinSet<()>,
}

impl TestPool {
    async fn new(max_age: Option<Duration>, ping_interval: Option<Duration>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = Endpoint::from_shared(format!("http://{}", listener.local_addr().unwrap()))
            .unwrap()
            .timeout(Duration::from_secs(1));
        let incoming = futures_util::stream::unfold(listener, |listener| async {
            Some((listener.accept().await.map(|(stream, _)| stream), listener))
        });
        let (requests, rx) = unbounded_channel();
        let fail_next = Arc::new(AtomicBool::new(false));
        let service = PingService {
            requests,
            fail_next: fail_next.clone(),
        };
        let mut tasks = tokio::task::JoinSet::new();
        tasks.spawn(async move {
            tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_incoming(incoming)
                .await
                .unwrap();
        });

        let (tx, rx_changes) = channel(3);
        let balance = Balance::new(ChannelStream::new(rx_changes));
        let (transport, worker) = Buffer::pair(balance, 1024);
        tasks.spawn(worker);
        let provider = Arc::new(TestTokenProvider);
        let client = create_client(box_transport(transport), Some(provider.clone()), true);
        let mut manager = ChannelManager::new(
            endpoint,
            provider,
            "projects/test-project/instances/test-instance".into(),
            3,
            false,
            Some("test-profile".into()),
            max_age,
            ping_interval,
            tx,
            client,
        );
        manager.seed().await.unwrap();
        Self {
            manager,
            requests: rx,
            fail_next,
            tasks,
        }
    }
}

async fn next_ping(requests: &mut UnboundedReceiver<Ping>) -> Ping {
    timeout(Duration::from_secs(3), requests.recv())
        .await
        .expect("no PingAndWarm received")
        .unwrap()
}

#[tokio::test]
async fn periodic_warming_visits_every_channel_in_order_despite_rpc_failure() {
    let interval = Duration::from_millis(50);
    let TestPool {
        manager,
        mut requests,
        fail_next,
        mut tasks,
    } = TestPool::new(None, Some(interval)).await;
    fail_next.store(true, Ordering::SeqCst);
    let started = Instant::now();
    tasks.spawn(manager.run());

    let mut peers = Vec::new();
    for _ in 0..6 {
        let ping = next_ping(&mut requests).await;
        assert!(ping.at >= started + interval);
        assert_eq!(
            ping.request.name,
            "projects/test-project/instances/test-instance"
        );
        assert_eq!(ping.request.app_profile_id, "test-profile");
        peers.push(ping.peer);
    }
    assert_eq!(peers[..3].iter().collect::<HashSet<_>>().len(), 3);
    assert_eq!(peers[..3], peers[3..]);
}

#[tokio::test]
async fn none_and_zero_disable_periodic_warming() {
    for interval in [None, Some(Duration::ZERO)] {
        let TestPool {
            manager,
            mut requests,
            tasks: _tasks,
            ..
        } = TestPool::new(None, interval).await;
        timeout(Duration::from_secs(1), manager.run())
            .await
            .unwrap();
        assert!(requests.try_recv().is_err());
    }
}

#[tokio::test]
async fn rejected_refresh_keeps_warming_the_original_channels() {
    let mut pool = TestPool::new(None, None).await;
    pool.manager.ping_and_warm_channels().await;
    let mut original = Vec::new();
    for _ in 0..3 {
        original.push(next_ping(&mut pool.requests).await.peer);
    }

    // Direct pings do not drain the seeded discovery queue, so all replacements are rejected.
    assert_eq!(pool.manager.change_sender.capacity(), 0);
    pool.manager.refresh_channels().await;
    pool.manager.ping_and_warm_channels().await;
    let mut after_refresh = Vec::new();
    for _ in 0..3 {
        after_refresh.push(next_ping(&mut pool.requests).await.peer);
    }
    assert_eq!(original, after_refresh);
}

#[tokio::test]
async fn idle_pool_keeps_refreshing_and_warming_replacement_channels() {
    let TestPool {
        manager,
        mut requests,
        mut tasks,
        ..
    } = TestPool::new(
        Some(Duration::from_millis(150)),
        Some(Duration::from_millis(30)),
    )
    .await;
    tasks.spawn(manager.run());

    // Three distinct connections per generation, covering the initial pool and two refreshes.
    // No application request drives Balance; the manager must drain discovery itself.
    timeout(Duration::from_secs(3), async {
        let mut peers = HashSet::new();
        while peers.len() < 9 {
            peers.insert(requests.recv().await.unwrap().peer);
        }
    })
    .await
    .expect("refresh or warming stopped progressing on an idle pool");
}
