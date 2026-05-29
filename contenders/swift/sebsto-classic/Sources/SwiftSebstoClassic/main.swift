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

// One shared HTTPClient + AWSClient + S3 client, instantiated once at cold
// start so subsequent warm invocations skip the connection setup.
let httpClient = HTTPClient.shared
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
