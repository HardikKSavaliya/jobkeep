//! The shared `Storage` conformance suite, against a real Postgres.

mod common;

jobkeep_core::storage_conformance_tests!(match common::storage().await {
    Some(storage) => storage,
    None => return,
});
