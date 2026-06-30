//! Library facade for the figment-engine contender.
//!
//! The Lambda binary (`main.rs`) is unchanged and self-contained — it keeps its
//! own `mod engine; mod aws;` and builds into `bootstrap` exactly as before.
//!
//! This `lib.rs` is purely additive: it exposes the generic, reusable modules so
//! that sibling contenders (e.g. `figment-engine-chain`) can depend on this crate
//! by path and SHARE the ZIP byte-layout, CRC decoding, planner vocabulary and S3
//! helpers, rather than duplicating them. Nothing here changes how the binary is
//! compiled or run.
//!
//! Build note: `cargo lambda build --package figment-engine` still produces the
//! binary's `bootstrap` from `main.rs`. A lib + bin in one package is standard;
//! the lib target is only compiled when a dependent (the chain contender) needs
//! it. The engine/aws sources are simply included by both targets.

pub mod aws;
pub mod engine;

// Convenience re-exports of the shared planner vocabulary, so dependents can
// write `figment_engine::{SourceFile, FileId}` instead of the full path.
pub use engine::plan::{FileId, SourceFile};
