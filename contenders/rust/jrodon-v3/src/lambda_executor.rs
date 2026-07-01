//! Fan-out executor that distributes part jobs across multiple sub-worker Lambda invocations.
//!
//! [`LambdaExecutor`] manages a pool of [`LambdaWorker`]s. Each worker runs in its own Tokio
//! task, warms up its Lambda instance, then processes [`DelegatedJobs`] batches sequentially.
//! The orchestrator sends commands through a bounded channel; results (completed parts and
//! Central Directory headers) flow back through unbounded channels to [`LambdaExecutor::wait_all`].

use std::sync::Arc;

use aws_sdk_lambda::types::InvocationType;
use aws_sdk_s3::primitives::Blob;
use tracing::{info, instrument};

use tokio::{
    sync::mpsc::{Sender, UnboundedReceiver, UnboundedSender, channel, unbounded_channel},
    task::JoinHandle,
};

use crate::{
    LAMBDA_WORKER_PART_JOB_MAX_CHUNK_SIZE,
    error::Error,
    events::{DelegatedJobs, Invocation, JobsResults},
    lambda,
    part_executor::DelegatedPartJob,
    s3_ops::{CompletedPartInfo, MultipartUpload},
    zip_format::CentralDirectoryFileHeader,
};

/// Commands sent to the [`LambdaExecutor`] dispatch task through its bounded channel.
pub enum LambdaExecutorCommand {
    /// Provides the open multipart upload context; must be sent exactly once, before any jobs.
    MultipartUpload(MultipartUpload),
    /// A batch of serializable part jobs to distribute across the worker pool.
    DelegatedPartJobs(Vec<DelegatedPartJob>),
}

/// Orchestrates fan-out of part jobs across a pool of sub-worker Lambda invocations.
///
/// Internally spawns a dispatch task that receives [`LambdaExecutorCommand`]s and distributes
/// job batches fairly across [`LambdaWorker`]s. Results are collected via unbounded channels
/// and returned by [`wait_all`](LambdaExecutor::wait_all).
pub struct LambdaExecutor {
    tx: Sender<LambdaExecutorCommand>,
    cdfh_rx: UnboundedReceiver<Vec<CentralDirectoryFileHeader>>,
    completed_part_rx: UnboundedReceiver<Vec<CompletedPartInfo>>,
    handle: JoinHandle<Result<(), Error>>,
}

impl LambdaExecutor {
    /// Creates a new executor with `worker_count` sub-worker tasks, each targeting `lambda_name`.
    ///
    /// Spawns the dispatch task and all workers immediately; workers warm up their Lambda
    /// instances concurrently before accepting job batches.
    pub fn new(lambda_name: Arc<str>, worker_count: usize) -> Self {
        let (tx, mut rx) = channel(1);
        let (completed_part_tx, completed_part_rx) = unbounded_channel();
        let (cdfh_tx, cdfh_rx) = unbounded_channel();

        let workers = (0..worker_count)
            .map(|id| {
                LambdaWorker::new(
                    lambda_name.clone(),
                    id,
                    cdfh_tx.clone(),
                    completed_part_tx.clone(),
                )
            })
            .collect::<Vec<_>>();

        Self {
            tx,
            cdfh_rx,
            completed_part_rx,
            handle: tokio::spawn(async move {
                info!("Lambda Executor started");

                let Some(LambdaExecutorCommand::MultipartUpload(multipart_upload)) =
                    rx.recv().await
                else {
                    return Err("The first LambdaExecutorCommand must be MultipartUpload")?;
                };

                while let Some(cmd) = rx.recv().await {
                    match cmd {
                        LambdaExecutorCommand::DelegatedPartJobs(mut delegated_part_jobs) => {
                            info!(
                                "delegated_part_jobs.len()" = delegated_part_jobs.len(),
                                "Lambda Executor sending jobs to worker"
                            );

                            // Fair job distribution: assign jobs to workers one at a time in
                            // round-robin order. Because jobs are sorted largest-to-smallest,
                            // simple chunking would give one worker all the heavy jobs; this
                            // interleaving spreads the load more evenly.
                            //
                            // Each round allocates one job list per worker. When all lists in a
                            // round reach LAMBDA_WORKER_PART_JOB_MAX_CHUNK_SIZE, a new round of
                            // lists is started and distribution continues until all jobs are
                            // assigned.
                            let mut worker_jobs = vec![];
                            'outer: loop {
                                worker_jobs.push(
                                    (0..worker_count)
                                        .map(|_| {
                                            Vec::with_capacity(
                                                (delegated_part_jobs.len() / worker_count + 1)
                                                    .min(LAMBDA_WORKER_PART_JOB_MAX_CHUNK_SIZE),
                                            )
                                        })
                                        .collect::<Vec<_>>(),
                                );
                                let filling_worker_jobs =
                                    worker_jobs.last_mut().expect("not empty");
                                loop {
                                    for worker_jobs in filling_worker_jobs.iter_mut() {
                                        if let Some(delegated_part_job) = delegated_part_jobs.pop()
                                        {
                                            worker_jobs.push(delegated_part_job);
                                        } else {
                                            break 'outer;
                                        }
                                    }
                                    if filling_worker_jobs.first().expect("not empty").len()
                                        >= LAMBDA_WORKER_PART_JOB_MAX_CHUNK_SIZE
                                    {
                                        break;
                                    }
                                }
                            }

                            for (delegated_jobs, worker) in worker_jobs
                                .into_iter()
                                .flatten()
                                .map(|jobs| DelegatedJobs {
                                    multipart_upload: multipart_upload.clone(),
                                    jobs,
                                })
                                .zip(workers.iter().cycle())
                            {
                                worker
                                    .job_tx
                                    .send(delegated_jobs)
                                    .await
                                    .map_err(|_| "Could not send the Job: Channel closed")?;
                            }
                        }
                        LambdaExecutorCommand::MultipartUpload(_) => {
                            Err("The only the first LambdaExecutorCommand can be MultipartUpload")?
                        }
                    }
                }

                info!("Jobs channel closed, Lambda Executor waiting for workers to finish");
                for worker in workers {
                    // Need to drop so the worker task exits its main loop
                    drop(worker.job_tx);
                    worker.handle.await??;
                }
                info!("Lambda Executor stopped");
                Ok(())
            }),
        }
    }

    /// Sends a batch of delegated part jobs to the dispatch task for distribution.
    ///
    /// Must be called after [`set_multipart_upload`](Self::set_multipart_upload).
    pub async fn submit_jobs(
        &self,
        delegated_part_jobs: Vec<DelegatedPartJob>,
    ) -> Result<(), Error> {
        self.tx
            .send(LambdaExecutorCommand::DelegatedPartJobs(
                delegated_part_jobs,
            ))
            .await
            .map_err(|_| "Could not send the DelegatedPartJobs: Channel closed")?;
        Ok(())
    }

    /// Sends the open multipart upload context to the dispatch task.
    ///
    /// Must be called exactly once, before [`submit_jobs`](Self::submit_jobs).
    pub async fn set_multipart_upload(
        &self,
        multipart_upload: MultipartUpload,
    ) -> Result<(), Error> {
        self.tx
            .send(LambdaExecutorCommand::MultipartUpload(multipart_upload))
            .await
            .map_err(|_| "Could not send the MultipartUpload: Channel closed")?;
        Ok(())
    }

    /// Signals that no more jobs will be submitted, then waits for all workers to finish.
    ///
    /// Drops the command sender so the dispatch task exits its receive loop, which in turn
    /// drops each worker's job sender, triggering the workers to drain their queues and stop.
    /// Returns the merged completed parts and Central Directory headers from all workers.
    #[instrument(skip_all)]
    pub async fn wait_all(
        self,
    ) -> Result<(Vec<CompletedPartInfo>, Vec<CentralDirectoryFileHeader>), Error> {
        let LambdaExecutor {
            tx,
            mut cdfh_rx,
            mut completed_part_rx,
            handle,
            ..
        } = self;

        info!("Lambda Executor dropping Job TX");
        // We need to drop the tx, so the dispatch task stops and is dropped
        // which causes the job_tx of the Lambda workers to be dropped
        // which triggers the last batch to start processing.
        drop(tx);

        let mut completed_parts = vec![];
        let mut cdfhs = vec![];

        tokio::join!(
            async {
                while let Some(new_completed_parts) = completed_part_rx.recv().await {
                    completed_parts.extend(new_completed_parts);
                }
            },
            async {
                while let Some(new_cdfhs) = cdfh_rx.recv().await {
                    cdfhs.extend(new_cdfhs);
                }
            }
        );

        // The executor dispatch task should have stopped
        handle.await??;

        Ok((completed_parts, cdfhs))
    }
}

/// A single sub-worker that warms up a Lambda instance and then processes [`DelegatedJobs`]
/// batches sequentially, forwarding results through shared unbounded channels.
struct LambdaWorker {
    /// Sender used by the dispatch task to push job batches to this worker.
    job_tx: Sender<DelegatedJobs>,
    handle: JoinHandle<Result<(), Error>>,
}

impl LambdaWorker {
    /// Spawns the worker task: warms up the Lambda instance, then processes incoming batches.
    fn new(
        lambda_name: Arc<str>,
        id: usize,
        cdfh_tx: UnboundedSender<Vec<CentralDirectoryFileHeader>>,
        completed_part_tx: UnboundedSender<Vec<CompletedPartInfo>>,
    ) -> Self {
        let (job_tx, mut job_rx) = channel(1);

        Self {
            job_tx,
            handle: tokio::spawn(async move {
                info!(id, "Lambda Executor worker started");
                Self::warmup_lambda((*lambda_name).to_owned()).await?;
                info!(id, "Lambda Executor worker finished warming-up");

                while let Some(delegated_jobs) = job_rx.recv().await {
                    info!(
                        id,
                        "delegated_jobs.jobs.len()" = delegated_jobs.jobs.len(),
                        "Lambda Executor worker batch processing starts"
                    );
                    Self::process_delegated_jobs(
                        (*lambda_name).to_owned(),
                        delegated_jobs,
                        &cdfh_tx,
                        &completed_part_tx,
                    )
                    .await?;
                }
                info!(id, "Lambda Executor worker stopped");
                Ok(())
            }),
        }
    }

    /// Invokes the Lambda with a `Warm` payload to trigger container initialization.
    async fn warmup_lambda(lambda_name: String) -> Result<(), Error> {
        lambda()
            .invoke()
            .function_name(lambda_name)
            .invocation_type(InvocationType::RequestResponse)
            .payload(Blob::new(serde_json::to_vec(&Invocation::Warm)?))
            .send()
            .await
            .map_err(|e| Box::new(aws_sdk_lambda::Error::from(e)))?;
        Ok(())
    }
    /// Invokes the Lambda with a `DelegatedJobs` payload and forwards the results.
    ///
    /// Deserializes the [`JobsResults`] from the response payload and sends the CDFHs and
    /// completed parts to the shared result channels.
    async fn process_delegated_jobs(
        lambda_name: String,
        delegated_jobs: DelegatedJobs,
        cdfh_tx: &UnboundedSender<Vec<CentralDirectoryFileHeader>>,
        completed_part_tx: &UnboundedSender<Vec<CompletedPartInfo>>,
    ) -> Result<(), Error> {
        let output = lambda()
            .invoke()
            .function_name(lambda_name)
            .invocation_type(InvocationType::RequestResponse)
            .payload(Blob::new(serde_json::to_vec(&delegated_jobs)?))
            .send()
            .await
            .map_err(|e| Box::new(aws_sdk_lambda::Error::from(e)))?;

        let JobsResults {
            cdfhs,
            completed_parts,
        } = serde_json::from_slice(
            &output
                .payload
                .ok_or("Lambda worker did not return a response")?
                .into_inner(),
        )?;

        cdfh_tx
            .send(cdfhs)
            .map_err(|_| "Could not send the Job Response: Channel closed")?;
        completed_part_tx
            .send(completed_parts.into_iter().collect())
            .map_err(|_| "Could not send the Job Response: Channel closed")?;

        Ok(())
    }
}
