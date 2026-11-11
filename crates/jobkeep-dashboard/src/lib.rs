//! A web dashboard for JobKeep: job counts, job lists and details, and
//! requeue/delete actions, over any [`Storage`] backend.
//!
//! ```ignore
//! let app = axum::Router::new()
//!     .nest("/jobs", jobkeep_dashboard::router(storage.clone()))
//!     .layer(your_auth_layer);
//! ```
//!
//! The dashboard has **no authentication of its own**. Put it behind your
//! app's auth, or bind it to localhost only.
//!
//! Routes, relative to where it is mounted:
//!
//! | Method | Path                     | Does                                 |
//! |--------|--------------------------|--------------------------------------|
//! | GET    | `/`                      | the HTML page                        |
//! | GET    | `/api/stats`             | job counts by state                  |
//! | GET    | `/api/jobs`              | jobs, newest first: `?state=&queue=&job_type=&limit=&offset=` |
//! | GET    | `/api/jobs/{id}`         | one job                              |
//! | POST   | `/api/jobs/{id}/requeue` | run a finished job again             |
//! | POST   | `/api/jobs/{id}/delete`  | delete a job                         |
//!
//! The two POST routes require an `x-jobkeep-action` header. Browsers only
//! send custom headers cross-origin after a CORS preflight, which this
//! router never approves, so another site cannot trigger them.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::Json;
use axum::Router;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use jobkeep_core::{JobFilter, JobId, JobRecord, Queue, Stats, Storage, StorageError};
use serde::{Deserialize, Serialize};

/// Header the mutating routes require.
pub const ACTION_HEADER: &str = "x-jobkeep-action";

const INDEX_HTML: &str = include_str!("index.html");

/// Builds the dashboard's routes. Mount it with [`Router::nest`] or serve it
/// directly.
pub fn router<S: Storage>(storage: S) -> Router {
    Router::new()
        .route("/", get(index))
        .route("/api/stats", get(stats::<S>))
        .route("/api/jobs", get(list::<S>))
        .route("/api/jobs/{id}", get(show::<S>))
        .route("/api/jobs/{id}/requeue", post(requeue::<S>))
        .route("/api/jobs/{id}/delete", post(delete::<S>))
        .with_state(Arc::new(storage))
}

/// Serves the dashboard on `addr` until `shutdown` resolves.
pub async fn serve<S: Storage>(
    storage: S,
    addr: SocketAddr,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> std::io::Result<()> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(
        "jobkeep dashboard listening on http://{}",
        listener.local_addr()?
    );
    axum::serve(listener, router(storage))
        .with_graceful_shutdown(shutdown)
        .await
}

/// An error response: `{"error": "..."}` with a fitting status.
#[derive(Debug)]
struct ApiError(StatusCode, String);

impl From<StorageError> for ApiError {
    fn from(e: StorageError) -> Self {
        match e {
            StorageError::NotFound(_) => Self(StatusCode::NOT_FOUND, e.to_string()),
            StorageError::InvalidTransition { .. } | StorageError::LeaseLost(_) => {
                Self(StatusCode::CONFLICT, e.to_string())
            }
            StorageError::Backend(_) => {
                tracing::error!("dashboard storage error: {e}");
                Self(StatusCode::INTERNAL_SERVER_ERROR, "storage error".into())
            }
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        #[derive(Serialize)]
        struct Body {
            error: String,
        }
        (self.0, Json(Body { error: self.1 })).into_response()
    }
}

type ApiResult<T> = Result<T, ApiError>;

async fn index() -> impl IntoResponse {
    (
        [
            (header::CONTENT_TYPE, "text/html; charset=utf-8"),
            (header::X_FRAME_OPTIONS, "DENY"),
            (
                header::CONTENT_SECURITY_POLICY,
                "default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; \
                 connect-src 'self'; frame-ancestors 'none'",
            ),
        ],
        Html(INDEX_HTML),
    )
}

async fn stats<S: Storage>(State(storage): State<Arc<S>>) -> ApiResult<Json<Stats>> {
    Ok(Json(storage.stats().await?))
}

#[derive(Debug, Default, Deserialize)]
struct ListParams {
    state: Option<String>,
    queue: Option<String>,
    job_type: Option<String>,
    limit: Option<u32>,
    offset: Option<u32>,
}

/// Treats `?state=` (empty) like an absent filter, as HTML forms send it.
fn non_empty(s: Option<String>) -> Option<String> {
    s.filter(|s| !s.is_empty())
}

async fn list<S: Storage>(
    State(storage): State<Arc<S>>,
    Query(params): Query<ListParams>,
) -> ApiResult<Json<Vec<JobRecord>>> {
    let mut filter = JobFilter::new();
    if let Some(state) = non_empty(params.state) {
        filter.state = Some(
            state
                .parse()
                .map_err(|e| ApiError(StatusCode::BAD_REQUEST, format!("{e}")))?,
        );
    }
    filter.queue = non_empty(params.queue).map(Queue::new);
    filter.job_type = non_empty(params.job_type);
    if let Some(limit) = params.limit {
        filter.limit = limit;
    }
    filter.offset = params.offset.unwrap_or(0);
    Ok(Json(storage.list(&filter).await?))
}

fn parse_id(id: &str) -> ApiResult<JobId> {
    id.parse()
        .map_err(|_| ApiError(StatusCode::BAD_REQUEST, format!("invalid job id {id:?}")))
}

async fn show<S: Storage>(
    State(storage): State<Arc<S>>,
    Path(id): Path<String>,
) -> ApiResult<Json<JobRecord>> {
    let id = parse_id(&id)?;
    storage
        .get(id)
        .await?
        .map(Json)
        .ok_or_else(|| StorageError::NotFound(id).into())
}

fn require_action_header(headers: &HeaderMap) -> ApiResult<()> {
    if headers.contains_key(ACTION_HEADER) {
        Ok(())
    } else {
        Err(ApiError(
            StatusCode::FORBIDDEN,
            format!("missing {ACTION_HEADER} header"),
        ))
    }
}

async fn requeue<S: Storage>(
    State(storage): State<Arc<S>>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> ApiResult<StatusCode> {
    require_action_header(&headers)?;
    let id = parse_id(&id)?;
    storage.requeue(id).await?;
    tracing::info!(%id, "job requeued from dashboard");
    Ok(StatusCode::NO_CONTENT)
}

async fn delete<S: Storage>(
    State(storage): State<Arc<S>>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> ApiResult<StatusCode> {
    require_action_header(&headers)?;
    let id = parse_id(&id)?;
    storage.delete(id).await?;
    tracing::info!(%id, "job deleted from dashboard");
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::Request;
    use jobkeep_core::{NewJob, Outcome, State as JobState, WorkerId};
    use jobkeep_memory::MemoryStorage;
    use tower::ServiceExt;

    use super::*;

    fn job(queue: &str) -> NewJob {
        NewJob {
            job_type: "test".into(),
            payload: serde_json::json!({ "n": 1 }),
            queue: Queue::from(queue),
            priority: Default::default(),
            run_at: None,
            max_attempts: 3,
            parent: None,
            unique_key: None,
        }
    }

    async fn call(app: &Router, req: Request<Body>) -> (StatusCode, serde_json::Value) {
        let res = app.clone().oneshot(req).await.unwrap();
        let status = res.status();
        let bytes = axum::body::to_bytes(res.into_body(), 1 << 20)
            .await
            .unwrap();
        let json = if bytes.is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null)
        };
        (status, json)
    }

    fn get(uri: &str) -> Request<Body> {
        Request::get(uri).body(Body::empty()).unwrap()
    }

    fn action(uri: &str) -> Request<Body> {
        Request::post(uri)
            .header(ACTION_HEADER, "1")
            .body(Body::empty())
            .unwrap()
    }

    async fn setup() -> (Router, Arc<MemoryStorage>, JobId, JobId) {
        let storage = Arc::new(MemoryStorage::new());
        let waiting = storage.enqueue(job("default")).await.unwrap();
        let failed = storage.enqueue(job("emails")).await.unwrap();
        let worker = WorkerId::new();
        storage
            .fetch(
                &[Queue::from("emails")],
                worker,
                chrono::TimeDelta::seconds(30),
            )
            .await
            .unwrap()
            .unwrap();
        storage
            .complete(
                failed,
                worker,
                Outcome::Failed {
                    error: "boom".into(),
                },
            )
            .await
            .unwrap();
        (router(Arc::clone(&storage)), storage, waiting, failed)
    }

    #[tokio::test]
    async fn serves_page() {
        let (app, ..) = setup().await;
        let res = app.oneshot(get("/")).await.unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        assert!(res.headers().contains_key(header::CONTENT_SECURITY_POLICY));
        let bytes = axum::body::to_bytes(res.into_body(), 1 << 20)
            .await
            .unwrap();
        assert!(String::from_utf8_lossy(&bytes).contains("<title>JobKeep</title>"));
    }

    #[tokio::test]
    async fn stats_and_lists() {
        let (app, _, waiting, failed) = setup().await;

        let (status, stats) = call(&app, get("/api/stats")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(stats["counts"]["enqueued"], 1);
        assert_eq!(stats["counts"]["failed"], 1);

        let (_, all) = call(&app, get("/api/jobs")).await;
        assert_eq!(all.as_array().unwrap().len(), 2);
        assert_eq!(all[0]["id"], failed.to_string(), "newest first");

        let (_, by_state) = call(&app, get("/api/jobs?state=failed&queue=")).await;
        assert_eq!(by_state.as_array().unwrap().len(), 1);
        assert_eq!(by_state[0]["last_error"], "boom");

        let (_, by_queue) = call(&app, get("/api/jobs?queue=default&limit=10")).await;
        assert_eq!(by_queue[0]["id"], waiting.to_string());

        let (status, err) = call(&app, get("/api/jobs?state=bogus")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(err["error"].as_str().unwrap().contains("bogus"));
    }

    #[tokio::test]
    async fn shows_one_job() {
        let (app, _, waiting, _) = setup().await;
        let (status, job) = call(&app, get(&format!("/api/jobs/{waiting}"))).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(job["state"], "enqueued");
        assert_eq!(job["payload"]["n"], 1);

        let (status, _) = call(&app, get(&format!("/api/jobs/{}", JobId::new()))).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        let (status, _) = call(&app, get("/api/jobs/not-a-uuid")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn actions_need_header() {
        let (app, storage, _, failed) = setup().await;
        let bare = Request::post(format!("/api/jobs/{failed}/requeue"))
            .body(Body::empty())
            .unwrap();
        let (status, _) = call(&app, bare).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert_eq!(
            storage.get(failed).await.unwrap().unwrap().state,
            JobState::Failed
        );
    }

    #[tokio::test]
    async fn requeue_and_delete() {
        let (app, storage, waiting, failed) = setup().await;

        let (status, _) = call(&app, action(&format!("/api/jobs/{failed}/requeue"))).await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        assert_eq!(
            storage.get(failed).await.unwrap().unwrap().state,
            JobState::Enqueued
        );

        // Not finished, so it can't be requeued.
        let (status, err) = call(&app, action(&format!("/api/jobs/{waiting}/requeue"))).await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert!(err["error"].as_str().unwrap().contains("cannot move"));

        let (status, _) = call(&app, action(&format!("/api/jobs/{waiting}/delete"))).await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        assert_eq!(
            storage.get(waiting).await.unwrap().unwrap().state,
            JobState::Deleted
        );

        let (status, _) = call(&app, action(&format!("/api/jobs/{waiting}/delete"))).await;
        assert_eq!(status, StatusCode::CONFLICT);
    }
}
