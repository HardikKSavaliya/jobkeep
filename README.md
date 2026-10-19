# JobKeep

Background jobs, scheduling and recurring tasks for Rust, in the spirit of Hangfire.

## Next up

- **Registry and type erasure:** map `Job::NAME` to a boxed handler that deserializes the payload and calls `run`.
- **Memory storage:** an in-memory backend in `jobkeep-memory` to enqueue, fetch and transition jobs, for tests.
- **Macro skeleton:** make `#[job]` in `jobkeep-macros` generate the `Job` impl instead of passing the function through.
