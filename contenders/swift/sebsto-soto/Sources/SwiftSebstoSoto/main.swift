import AsyncHTTPClient
import AWSLambdaRuntime
import Logging
import SotoCore
import SotoS3

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

// One HTTPClient + AWSClient + S3 client, instantiated once at cold start
// so subsequent warm invocations skip the connection setup.
//
// Run-5 profiling showed downloader-bound runtime at ~12 MB/s per task with
// the default `HTTPClient.shared` (8 connections per host). With 10+
// concurrent S3 GETs, requests serialize behind the pool. Raise the soft
// limit and tighten timeouts so a misbehaving connection doesn't stall the
// whole pipeline.
var httpConfig = HTTPClient.Configuration()
httpConfig.connectionPool.concurrentHTTP1ConnectionsPerHostSoftLimit = 32
httpConfig.timeout.read = .seconds(120)
httpConfig.timeout.connect = .seconds(10)
let httpClient = HTTPClient(eventLoopGroupProvider: .singleton, configuration: httpConfig)
let awsClient = AWSClient(httpClient: httpClient)

let region: Region = {
    if let r = ProcessInfo.processInfo.environment["AWS_REGION"] {
        return Region(rawValue: r)
    }
    return .useast1
}()
let s3 = S3(client: awsClient, region: region)

let runtime = LambdaRuntime { (event: JobInfo, context: LambdaContext) async throws -> String in
    context.logger.info("event: bucket=\(event.bucket_name) prefix=\(event.files_prefix) archive=\(event.archive_key)")
    context.logger.info("tunables: lambdaMemory=\(lambdaMemoryMB)MB maxDownloadsMemory=\(Tunables.maxDownloadsMemory / 1024 / 1024)MiB")
    try await runArchiveJob(s3: s3, job: event, logger: context.logger)
    return "ok"
}

try await runtime.run()
