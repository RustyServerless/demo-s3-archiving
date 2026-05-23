//! CloudFormation Custom Resource that fills the benching S3 bucket
//! with `OBJECT_COUNT` random-content objects on `Create` events.
//!
//! On `Delete`, lists and deletes every object under `<key_prefix>`.
//! On `Update`, runs Delete then Create (effectively a refill).
//!
//! Each object:
//! - has a size drawn from a normal distribution centered on `SIZE_MEAN`
//!   with standard deviation `SIZE_STDDEV`, clamped to `[SIZE_MIN, SIZE_MAX]`;
//! - is filled with uniformly random bytes;
//! - is uploaded under the key `<key_prefix>/<sha256_hex>`.

use std::sync::Arc;

use aws_lambda_events::cloudformation::{
    CloudFormationCustomResourceRequest, CloudFormationCustomResourceResponse,
    CloudFormationCustomResourceResponseStatus,
};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{Delete, ObjectIdentifier};
use awssdk_instrumentation::lambda::{LambdaError, LambdaEvent};
use rand::Rng;
use rand_distr::{Distribution, Normal};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tracing::{error, info, instrument};

// ---------- Tunables ----------
const SIZE_STDDEV: usize = 1 * 1024 * 1024;
const MAX_DEVIATION: usize = 3 * SIZE_STDDEV; // +/- 3sigma;
const MIN_FILE_SIZE: usize = 512 * 1024; // 512KB;

const CONCURRENT_UPLOADS: usize = 64;

// ---------- CFN custom resource event types ----------

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "PascalCase")]
struct ResourceProperties {
    bucket_name: String,
    key_prefix: String,
    object_count: String,
    object_size_mb: String,
}

/// Fields we need from any CFN custom resource event variant, hoisted out of
/// the `aws_lambda_events` enum so the rest of the code is variant-agnostic.
struct CfnCommon {
    response_url: String,
    stack_id: String,
    request_id: String,
    logical_resource_id: String,
    physical_resource_id: String,
}

/// What the handler should do, after we collapse the CFN request variants.
#[derive(Debug)]
enum Work {
    /// Create: fill the bucket with new random objects.
    Create(ResourceProperties),
    /// Update: delete everything under the prefix, then fill again.
    Update {
        old: ResourceProperties,
        new: ResourceProperties,
    },
    /// Delete: empty everything under the prefix.
    Delete(ResourceProperties),
}

// ---------- Entry point ----------

#[instrument(skip_all, fields(request_type))]
async fn handler(
    event: LambdaEvent<CloudFormationCustomResourceRequest<ResourceProperties, ResourceProperties>>,
) -> Result<Value, LambdaError> {
    // Destructure once: extract the common fields plus the work-to-do for this variant.
    // The `physical_resource_id` for a Create is synthesized from the bucket name so it
    // stays stable across stack updates (CloudFormation uses it to track the resource).
    let (common, work): (CfnCommon, Work) = match event.payload {
        CloudFormationCustomResourceRequest::Create(r) => {
            tracing::Span::current().record("request_type", "Create");
            (
                CfnCommon {
                    response_url: r.response_url,
                    stack_id: r.stack_id,
                    request_id: r.request_id,
                    logical_resource_id: r.logical_resource_id,
                    physical_resource_id: format!(
                        "fill-bucket-{}",
                        r.resource_properties.bucket_name
                    ),
                },
                Work::Create(r.resource_properties),
            )
        }
        CloudFormationCustomResourceRequest::Update(r) => {
            tracing::Span::current().record("request_type", "Update");
            (
                CfnCommon {
                    response_url: r.response_url,
                    stack_id: r.stack_id,
                    request_id: r.request_id,
                    logical_resource_id: r.logical_resource_id,
                    physical_resource_id: r.physical_resource_id,
                },
                Work::Update {
                    old: r.old_resource_properties,
                    new: r.resource_properties,
                },
            )
        }
        CloudFormationCustomResourceRequest::Delete(r) => {
            tracing::Span::current().record("request_type", "Delete");
            (
                CfnCommon {
                    response_url: r.response_url,
                    stack_id: r.stack_id,
                    request_id: r.request_id,
                    logical_resource_id: r.logical_resource_id,
                    physical_resource_id: r.physical_resource_id,
                },
                Work::Delete(r.resource_properties),
            )
        }
        // `CloudFormationCustomResourceRequest` is `#[non_exhaustive]`, so the compiler
        // forces this catch-all even though CFN only ever sends the three variants above.
        _ => return Err("unsupported CFN custom resource request variant".into()),
    };

    info!(?work);

    let outcome: Result<(), String> = match work {
        Work::Create(props) => fill(props).await.map_err(|e| format!("{e:#}")),
        Work::Update { old, new } => {
            // Update = Delete then Create. Borrow the bucket/prefix so we can reuse
            // `props` for the refill that follows.
            let del = empty(old).await.map_err(|e| format!("{e:#}"));
            match del {
                Ok(()) => fill(new).await.map_err(|e| format!("{e:#}")),
                Err(e) => Err(e),
            }
        }
        Work::Delete(props) => empty(props).await.map_err(|e| format!("{e:#}")),
    };

    let (status, reason) = match &outcome {
        Ok(()) => (
            CloudFormationCustomResourceResponseStatus::Success,
            "ok".to_string(),
        ),
        Err(msg) => {
            error!(error = %msg, "fill-bucket failed");
            (
                CloudFormationCustomResourceResponseStatus::Failed,
                msg.clone(),
            )
        }
    };

    // We MUST send a response to CloudFormation; otherwise the stack hangs for ~1h.
    if let Err(e) = send_cfn_response(common, status, reason).await {
        error!(error = %e, "failed to send cfn-response");
        // The Lambda still returns Ok so the runtime doesn't retry; CloudFormation
        // will time out on its side if we couldn't notify it.
    }

    Ok(serde_json::json!({}))
}

// ---------- Fill logic ----------

#[instrument]
async fn fill(props: ResourceProperties) -> Result<(), LambdaError> {
    info!(
        bucket = %props.bucket_name,
        prefix = %props.key_prefix,
        object_count = %props.object_count,
        object_size_mb = %props.object_size_mb,
        "fill-bucket"
    );
    let key_prefix: Arc<str> = Arc::from(props.key_prefix);
    let bucket_name = props.bucket_name;
    let object_count = props.object_count.parse::<usize>()?;
    let object_size = props.object_size_mb.parse::<usize>()? * 1024 * 1024;

    let sem = Arc::new(Semaphore::new(CONCURRENT_UPLOADS));
    let mut set: JoinSet<Result<(), LambdaError>> = JoinSet::new();

    let mut remaining_size_budget = object_count * object_size;
    let mut rng = rand::rng();
    for remaining_objects in (1..=object_count).rev() {
        if remaining_objects % 100 == 0 || remaining_objects < 100 {
            info!(
                remaining_objects,
                remaining_size_budget,
                "spawned {}/{object_count} uploads",
                object_count - remaining_objects
            );
        }
        let permit = sem.clone().acquire_owned().await.expect("semaphore closed");

        // Sample size on the caller side: keeps the RNG single-threaded and cheap.
        let size = if remaining_objects > 1 {
            sample_size(&mut rng, remaining_size_budget, remaining_objects)
        } else {
            remaining_size_budget
        };
        remaining_size_budget -= size;

        // Generate random content.
        let mut buf = vec![0u8; size];
        rng.fill(buf.as_mut_slice());

        let key_prefix = key_prefix.clone();
        let bucket_name = bucket_name.clone();
        set.spawn(async move {
            let _permit = permit; // released on task end
            upload_one(buf, bucket_name, &key_prefix).await
        });
    }

    while let Some(res) = set.join_next().await {
        // Propagate the first error; other tasks continue running but their
        // outputs are discarded — Lambda will return failure regardless.
        res.map_err(|e| format!("join error: {e}"))??;
    }
    info!("all {object_count} objects uploaded");
    Ok(())
}

fn sample_size<R: Rng + ?Sized>(
    rng: &mut R,
    remaining_size_budget: usize,
    remaining_objects: usize,
) -> usize {
    let target_mean_size = remaining_size_budget as f64 / remaining_objects as f64;
    let v = Normal::new(target_mean_size, SIZE_STDDEV as f64)
        .expect("stddev > 0")
        .sample(rng)
        .round() as usize;

    let target_mean_size = target_mean_size.round() as usize;
    let minimum_size = (target_mean_size - MAX_DEVIATION).max(MIN_FILE_SIZE);
    let maximum_size = target_mean_size + MAX_DEVIATION;

    v.clamp(minimum_size, maximum_size)
}

async fn upload_one(buf: Vec<u8>, bucket: String, prefix: &str) -> Result<(), LambdaError> {
    // SHA256 of the random content -> key.
    let digest = Sha256::digest(&buf);
    let hex = to_hex(&digest);

    s3().put_object()
        .bucket(bucket)
        .key(format!("{prefix}/{hex}"))
        .body(ByteStream::from(buf))
        .send()
        .await?;
    Ok(())
}

// ---------- Empty logic ----------

/// Lists every object under `prefix` in `bucket` and deletes them in batches
/// of up to 1000 (the S3 `DeleteObjects` limit).
#[instrument]
async fn empty(props: ResourceProperties) -> Result<(), LambdaError> {
    info!("empty-bucket");
    let key_prefix: Arc<str> = Arc::from(props.key_prefix);
    let bucket_name = &props.bucket_name;

    let mut continuation_token: Option<String> = None;
    let mut total_deleted: usize = 0;

    loop {
        let mut req = s3()
            .list_objects_v2()
            .bucket(bucket_name)
            .prefix(key_prefix.as_ref());
        if let Some(token) = continuation_token.as_ref() {
            req = req.continuation_token(token);
        }
        let page = req.send().await?;

        let objects: Vec<ObjectIdentifier> = page
            .contents()
            .iter()
            .filter_map(|o| o.key().map(|k| k.to_owned()))
            .map(|key| {
                ObjectIdentifier::builder()
                    .key(key)
                    .build()
                    .expect("key is set")
            })
            .collect();

        if !objects.is_empty() {
            let batch_len = objects.len();
            let delete = Delete::builder()
                .set_objects(Some(objects))
                .quiet(true)
                .build()
                .expect("objects are set");

            s3().delete_objects()
                .bucket(bucket_name)
                .delete(delete)
                .send()
                .await?;

            total_deleted += batch_len;
            info!(total_deleted, "deleted batch of {batch_len} objects");
        }

        if page.is_truncated().unwrap_or(false) {
            continuation_token = page.next_continuation_token().map(|s| s.to_owned());
            if continuation_token.is_none() {
                // Defensive: truncated but no token — avoid infinite loop.
                break;
            }
        } else {
            break;
        }
    }

    info!(total_deleted, "prefix emptied");
    Ok(())
}

// ---------- cfn-response ----------

async fn send_cfn_response(
    common: CfnCommon,
    status: CloudFormationCustomResourceResponseStatus,
    reason: String,
) -> Result<(), LambdaError> {
    // CloudFormationCustomResourceResponse is `#[non_exhaustive]` so we cannot
    // build it via struct literal syntax from outside the crate. The crate's
    // `Default` impl + field-by-field assignment is the supported path.
    let mut body = CloudFormationCustomResourceResponse::default();
    body.status = status;
    body.reason = Some(reason);
    body.physical_resource_id = common.physical_resource_id;
    body.stack_id = common.stack_id;
    body.request_id = common.request_id;
    body.logical_resource_id = common.logical_resource_id;

    let payload = serde_json::to_string(&body)?;
    info!(?body.status, "sending cfn-response");

    let client = reqwest::Client::new();
    let resp = client
        .put(&common.response_url)
        // The CFN-presigned URL requires an empty Content-Type header.
        .header(reqwest::header::CONTENT_TYPE, "")
        .header(reqwest::header::CONTENT_LENGTH, payload.len().to_string())
        .body(payload)
        .send()
        .await?;
    if !resp.status().is_success() {
        return Err(format!("cfn-response non-2xx: {}", resp.status()).into());
    }
    Ok(())
}

// ---------- Hex (hand-rolled) ----------
fn to_hex(bytes: &[u8]) -> String {
    use core::fmt::Write;
    let mut s = String::with_capacity(2 * bytes.len());
    for byte in bytes {
        let _ = write!(s, "{:02x}", byte);
    }
    s
}

// This macro from `awssdk-instrumentation` generates the entire main() function:
// - Initializes the OTel tracer provider with X-Ray exporter
// - Sets up tracing-subscriber with JSON console output and OTel bridge
// - Loads AWS SDK config from environment
// - Creates an instrumented S3 client accessible via `s3()`
// - Wraps the Lambda runtime with the OTel Tower layer
// - Starts the Lambda runtime
//
// See: https://docs.rs/awssdk-instrumentation/latest/awssdk_instrumentation/macro.make_lambda_runtime.html
awssdk_instrumentation::make_lambda_runtime!(
    handler,
    trigger = awssdk_instrumentation::lambda::layer::OTelFaasTrigger::Other,
    s3() -> aws_sdk_s3::Client
);
