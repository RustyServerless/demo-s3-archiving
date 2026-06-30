//! figment-engine-chain — the copy-only segment-chain contender.
//!
//! Speed-focused sibling of `figment-engine`: every image body reaches the
//! archive by server-side UploadPartCopy (bar one 5 MiB bootstrap read), so the
//! design escapes the ENI bandwidth floor and is bound only by S3 control-plane
//! call rate.
//!
//! Wiring: lists the source objects, builds the shared `SourceFile` vocabulary,
//! plans the segment chain, and runs the executor. Planner is implemented;
//! `assemble_chain` is still a stub (returns Unimplemented) until built.

mod assemble_chain;
mod plan_chain;
mod rate_limit;

use std::sync::Arc;

use awssdk_instrumentation::lambda::{LambdaError, LambdaEvent};
use figment_engine::engine::plan::{FileId, SourceFile};
use serde::Deserialize;
use tracing::info;

#[derive(Debug, Deserialize, Clone)]
struct JobInfo {
    bucket_name: Arc<str>,
    files_prefix: Arc<str>,
    archive_key: Arc<str>,
}

/// List every object under `{files_prefix}/`, returning the shared `SourceFile`
/// per object. (The shipped contender's lister lives in its bin, not its lib, so
/// the chain carries its own — the only non-shared piece.)
async fn list_source_files(
    bucket: &str,
    files_prefix: &str,
) -> Result<Vec<SourceFile>, LambdaError> {
    let s3_prefix = format!("{files_prefix}/");
    let mut paginator = s3()
        .list_objects_v2()
        .bucket(bucket)
        .prefix(&s3_prefix)
        .into_paginator()
        .send();

    let mut out = Vec::new();
    let mut next_id: u32 = 0;
    while let Some(page) = paginator.next().await {
        let page = page?;
        for obj in page.contents() {
            let Some(key) = obj.key() else { continue };
            let Some(size) = obj.size() else { continue };
            let Some(name) = key.strip_prefix(&s3_prefix) else {
                continue;
            };
            if name.is_empty() {
                continue;
            }
            out.push(SourceFile {
                id: FileId(next_id),
                key: key.to_string(),
                name: name.to_string(),
                size: size as u64,
            });
            next_id += 1;
        }
    }
    Ok(out)
}

async fn handler(event: LambdaEvent<JobInfo>) -> Result<(), LambdaError> {
    let JobInfo {
        bucket_name,
        files_prefix,
        archive_key,
    } = event.payload;

    info!(%bucket_name, %files_prefix, %archive_key, "figment-engine-chain invoked");

    let files = list_source_files(&bucket_name, &files_prefix).await?;
    info!(count = files.len(), "listed source files");

    let plan = plan_chain::plan_segment_chain(files)?;
    info!(
        entries = plan.stats.entries,
        segments = plan.stats.segments,
        bigs = plan.stats.bigs,
        smalls = plan.stats.smalls,
        links = plan.stats.links,
        max_chain_depth = plan.stats.max_chain_depth,
        "planned segment chain"
    );

    assemble_chain::run(&s3(), &bucket_name, &files_prefix, &archive_key, plan).await?;

    info!("archive complete");
    Ok(())
}

awssdk_instrumentation::make_lambda_runtime!(
    handler,
    trigger = awssdk_instrumentation::lambda::layer::OTelFaasTrigger::Other,
    s3() -> aws_sdk_s3::Client
);
