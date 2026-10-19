//! Procedural macros for JobKeep.

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, ItemFn};

/// Marks an async function as a JobKeep job.
///
/// Currently a pass-through; will later generate the job registration and
/// (de)serialization glue against `::jobkeep_core`.
#[proc_macro_attribute]
pub fn job(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let func = parse_macro_input!(item as ItemFn);
    quote!(#func).into()
}

/// Marks a job as recurring on a cron schedule, e.g. `#[recurring("0 */5 * * * *")]`.
///
/// Currently a pass-through.
#[proc_macro_attribute]
pub fn recurring(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let func = parse_macro_input!(item as ItemFn);
    quote!(#func).into()
}