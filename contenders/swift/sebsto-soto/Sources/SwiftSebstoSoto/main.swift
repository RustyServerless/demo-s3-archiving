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

// One HTTPClient + AWSClient + S3 client, instantiated once at cold start so
// warm invocations skip connection setup.
//
// `concurrentHTTP1ConnectionsPerHostSoftLimit` is raised from the
// AsyncHTTPClient default of 8 because the pipeline issues download GETs
// and upload PUTs concurrently to the same host (S3); the default would
// serialise requests behind a too-small connection pool.
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
    try await runArchiveJob(s3: s3, job: event, logger: context.logger)
    return "ok"
}

try await runtime.run()
