import Logging

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

// One-shot profiling instrument. Each public method records a duration in
// nanoseconds for one event of one stage. At the end of a run, `report(...)`
// emits aggregate stats (count + sum + p50 + p95 + p99) per stage.
//
// Implemented as an actor purely for thread-safety: it's called only from
// non-hot paths (around per-file boundaries, around per-part uploads), so
// the actor hop cost is negligible (~3000 + ~1500 = ~4500 hops total over
// the whole run, vs the ~250000 frame-level hops we *don't* instrument).
actor Stats {
    enum Stage: String, CaseIterable {
        // Time inside `getObject` body iteration (download + CRC).
        case downloadFile
        // Time the zipper waits for the next downloaded file to arrive.
        case zipperQueueWait
        // Time inside `producer.appendCompound` (chunk producer back-pressure).
        case zipperAppend
        // Time inside `S3.uploadPart`.
        case uploadPart
        // Time the uploader waits for the next sealed chunk.
        case uploaderQueueWait
    }

    private var samples: [Stage: [UInt64]] = [:]

    func record(_ stage: Stage, ns: UInt64) {
        samples[stage, default: []].append(ns)
    }

    func report(logger: Logger) {
        for stage in Stage.allCases {
            guard let s = samples[stage], !s.isEmpty else { continue }
            let sorted = s.sorted()
            let n = sorted.count
            let sum = sorted.reduce(UInt64(0), +)
            let p50 = sorted[n / 2]
            let p95 = sorted[Int(Double(n) * 0.95)]
            let p99 = sorted[Int(Double(n) * 0.99)]
            let max = sorted[n - 1]
            logger.info(
                "stats[\(stage.rawValue)]: n=\(n) sum=\(sum / 1_000_000)ms p50=\(p50 / 1000)us p95=\(p95 / 1000)us p99=\(p99 / 1000)us max=\(max / 1000)us"
            )
        }
    }
}

// Monotonic nanosecond clock, in line with Swift's ContinuousClock but
// returning a UInt64 so we can store millions of samples cheaply.
@inlinable
func monoNs() -> UInt64 {
    var ts = timespec()
    clock_gettime(CLOCK_MONOTONIC, &ts)
    return UInt64(ts.tv_sec) * 1_000_000_000 + UInt64(ts.tv_nsec)
}
