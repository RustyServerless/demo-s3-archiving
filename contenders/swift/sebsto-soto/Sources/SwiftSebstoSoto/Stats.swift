import Logging
import NIOConcurrencyHelpers

#if os(macOS)
import Darwin.C
#elseif canImport(Glibc)
import Glibc
#elseif canImport(Musl)
import Musl
#endif

#if canImport(FoundationEssentials)
import FoundationEssentials
#else
import Foundation
#endif

/// Read once at cold start. Truthy values: `1`, `true`, `yes`
/// (case-insensitive). When false, every call site short-circuits via an
/// inlined `if Stats.enabled` check — no allocation, no lock acquisition,
/// no monoNs() reads.
let statsEnabled: Bool = {
    guard let v = ProcessInfo.processInfo.environment["STATS"]?.lowercased() else { return false }
    return v == "1" || v == "true" || v == "yes"
}()

/// Thread-safe collector for per-stage duration samples.
///
/// Implemented as a `final class` + `NIOLockedValueBox` rather than an
/// `actor` so the disabled-path guard at call sites is a single load +
/// branch (no actor hop, no allocation). With STATS=1 the lock is held
/// for the duration of one array append per recorded sample — negligible
/// vs. the wall-clock cost of the work being measured.
final class Stats: @unchecked Sendable {
    static let enabled: Bool = statsEnabled

    enum Stage: String, CaseIterable {
        /// Time inside the per-file `getObject` body iteration + CRC.
        case downloadFile
        /// Time of the single CRC pass at end of file.
        case downloadInFrame
        /// Time the zipper waits for the next downloaded file.
        case zipperQueueWait
        /// Time inside `producer.appendCompound` (file LFH + body + DD).
        case zipperAppend
        /// Time inside `S3.uploadPart`.
        case uploadPart
        /// Time the uploader waits for the next sealed chunk.
        case uploaderQueueWait
    }

    private struct State {
        var samples: [Stage: [UInt64]] = [:]
        // Time-weighted in-flight download gauge. The counter is sampled
        // at each (start, stop) event; integral = Σ value × (t - tPrev),
        // so `integral / total time` is the time-weighted mean.
        var downloadInFlight: Int = 0
        var downloadInFlightMax: Int = 0
        var downloadInFlightLastT: UInt64 = 0
        var downloadInFlightIntegral: UInt64 = 0
        // Counter of `ChunkProducer.appendCompound` actor hops.
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

    /// Hot path. Call sites guard with `if Stats.enabled` before calling;
    /// the guard inside is a redundant safety net.
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

        // Peak RSS via getrusage. `ru_maxrss` is KB on Linux and bytes on
        // Darwin. On glibc, `RUSAGE_SELF` is imported as a `__rusage_who`
        // enum but `getrusage` takes `__rusage_who_t` (Int32); cast via
        // rawValue. On Darwin, `RUSAGE_SELF` is already an Int32 macro.
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
    }
}

/// Monotonic nanosecond clock via `clock_gettime(CLOCK_MONOTONIC)`.
/// Reading once costs ~30 ns on Graviton.
@inlinable
func monoNs() -> UInt64 {
    var ts = timespec()
    clock_gettime(CLOCK_MONOTONIC, &ts)
    return UInt64(ts.tv_sec) &* 1_000_000_000 &+ UInt64(ts.tv_nsec)
}
