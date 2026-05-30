import Logging
import NIOConcurrencyHelpers

#if os(macOS)
import Darwin.C
#elseif canImport(Glibc)
import Glibc
import CCRC32  // ccrc32_mallinfo_uordblks shim — Linux-only, hence the conditional import
#elseif canImport(Musl)
import Musl
#endif

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

// Read once at cold start. Truthy values: "1", "true", "yes" (case-insensitive).
//
// Public on the `Stats` type so call sites can gate the timer setup itself —
// not just the recording. Pattern at every hot-path call site:
//
//     if Stats.enabled {
//         let t0 = monoNs()
//         … work …
//         stats.record(.downloadFile, ns: monoNs() - t0)
//     } else {
//         … work …
//     }
//
// When STATS=0 the entire timing/dispatch path is skipped — no monoNs(),
// no record(), no lock acquisition.
let statsEnabled: Bool = {
    guard let v = ProcessInfo.processInfo.environment["STATS"]?.lowercased() else { return false }
    return v == "1" || v == "true" || v == "yes"
}()

// Thread-safe collector for per-stage duration samples.
//
// Replaces an earlier `actor Stats` because the actor hop was paid even when
// STATS=0 (the `guard statsEnabled` lived inside the actor body, so the
// dispatch already happened). With a class + lock and the guard hoisted to
// call sites, the disabled path is a single load + branch — no allocation,
// no actor scheduling.
//
// When enabled: the lock is held for the duration of one array append per
// recorded sample. ~9000 file-scoped samples + ~1500 part-scoped over a
// 3000-file run; negligible vs the wall-clock.
final class Stats: @unchecked Sendable {
    static let enabled: Bool = statsEnabled

    enum Stage: String, CaseIterable {
        case downloadFile           // Time inside `getObject` body collect + CRC.
        case downloadInFrame        // Time of the CRC pass over the collected body.
        case zipperQueueWait        // Time the zipper waits for the next downloaded file.
        case zipperAppend           // Time inside `producer.appendCompound`.
        case uploadPart             // Time inside `S3.uploadPart`.
        case uploaderQueueWait      // Time the uploader waits for the next sealed chunk.
    }

    private struct State {
        var samples: [Stage: [UInt64]] = [:]
        // Time-weighted in-flight gauges. We sample a counter at each
        // (start, stop) event; integral = Σ value * (t - tPrev).
        var downloadInFlight: Int = 0
        var downloadInFlightMax: Int = 0
        var downloadInFlightLastT: UInt64 = 0
        var downloadInFlightIntegral: UInt64 = 0
        // Hop counter for ChunkProducer.appendCompound. Tests H3.
        var producerHops: UInt64 = 0
    }

    private let state = NIOLockedValueBox(State())

    init(estimatedFiles: Int = 0, estimatedParts: Int = 0) {
        guard Stats.enabled else { return }
        state.withLockedValue { s in
            // Pre-reserve so the first hot-path append doesn't reallocate.
            for stage in Stage.allCases {
                let cap: Int
                switch stage {
                case .uploadPart, .uploaderQueueWait: cap = estimatedParts
                default: cap = estimatedFiles
                }
                if cap > 0 {
                    var arr: [UInt64] = []
                    arr.reserveCapacity(cap)
                    s.samples[stage] = arr
                }
            }
            s.downloadInFlightLastT = monoNs()
        }
    }

    // Hot path. Only reached from call sites that have already checked
    // `Stats.enabled`, so the guard inside is a redundant safety net.
    @inline(__always)
    func record(_ stage: Stage, ns: UInt64) {
        guard Stats.enabled else { return }
        state.withLockedValue { s in
            s.samples[stage, default: []].append(ns)
        }
    }

    func incrementInFlight() {
        guard Stats.enabled else { return }
        let now = monoNs()
        state.withLockedValue { s in
            s.downloadInFlightIntegral &+= UInt64(s.downloadInFlight) &* (now &- s.downloadInFlightLastT)
            s.downloadInFlightLastT = now
            s.downloadInFlight += 1
            if s.downloadInFlight > s.downloadInFlightMax { s.downloadInFlightMax = s.downloadInFlight }
        }
    }

    func decrementInFlight() {
        guard Stats.enabled else { return }
        let now = monoNs()
        state.withLockedValue { s in
            s.downloadInFlightIntegral &+= UInt64(s.downloadInFlight) &* (now &- s.downloadInFlightLastT)
            s.downloadInFlightLastT = now
            s.downloadInFlight -= 1
        }
    }

    func bumpProducerHops(_ n: Int = 1) {
        guard Stats.enabled else { return }
        state.withLockedValue { $0.producerHops &+= UInt64(n) }
    }

    func report(logger: Logger) {
        guard Stats.enabled else { return }

        let snapshot = state.withLockedValue { s -> State in
            // Close the in-flight integral at report time.
            var copy = s
            let now = monoNs()
            copy.downloadInFlightIntegral &+= UInt64(s.downloadInFlight) &* (now &- s.downloadInFlightLastT)
            copy.downloadInFlightLastT = now
            return copy
        }

        for stage in Stage.allCases {
            guard let s = snapshot.samples[stage], !s.isEmpty else { continue }
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

        // In-flight download gauge. Time-weighted mean = integral / total time.
        let totalNs = snapshot.downloadInFlightLastT
        if totalNs > 0 {
            let mean = Double(snapshot.downloadInFlightIntegral) / Double(totalNs)
            logger.info(
                "stats[downloadInFlight]: mean=\(String(format: "%.2f", mean)) max=\(snapshot.downloadInFlightMax)"
            )
        }

        if snapshot.producerHops > 0 {
            logger.info("stats[producerHops]: total=\(snapshot.producerHops)")
        }

        // Peak RSS. ru_maxrss is KB on Linux, bytes on Darwin.
        // On glibc, RUSAGE_SELF is imported as a `__rusage_who` enum but
        // getrusage takes `__rusage_who_t` (Int32) — cast through rawValue.
        // On Darwin RUSAGE_SELF is already an Int32 (a #define).
        var ru = rusage()
        #if canImport(Glibc) || canImport(Musl)
        let who = __rusage_who_t(RUSAGE_SELF.rawValue)
        #else
        let who = RUSAGE_SELF
        #endif
        if getrusage(who, &ru) == 0 {
            #if os(Linux)
            let peakMB = Double(ru.ru_maxrss) / 1024.0
            #else
            let peakMB = Double(ru.ru_maxrss) / (1024.0 * 1024.0)
            #endif
            logger.info("stats[peakRSS]: \(String(format: "%.1f", peakMB))MB")
        }

        #if os(Linux)
        // mallinfo2 is the modern variant (>=glibc 2.33). AL2023 ships glibc 2.34.
        // Accessed via the C wrapper; if unavailable at link time the wrapper
        // returns zeros. See CCRC32/ccrc32.c — same target also exposes a
        // mallinfo2 shim to keep us from adding a second C target.
        let heapBytes = ccrc32_mallinfo_uordblks()
        if heapBytes > 0 {
            let heapMB = Double(heapBytes) / (1024.0 * 1024.0)
            logger.info("stats[heapInUse]: \(String(format: "%.1f", heapMB))MB")
        }
        #endif
    }
}

// Monotonic nanosecond clock via clock_gettime(CLOCK_MONOTONIC). Pattern
// adapted from swift-aws-lambda-runtime/Sources/AWSLambdaRuntime/LambdaClock.swift.
// Reading once costs ~30 ns on Graviton2.
@inlinable
func monoNs() -> UInt64 {
    var ts = timespec()
    clock_gettime(CLOCK_MONOTONIC, &ts)
    return UInt64(ts.tv_sec) &* 1_000_000_000 &+ UInt64(ts.tv_nsec)
}
