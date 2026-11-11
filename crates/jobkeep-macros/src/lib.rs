//! Procedural macros for JobKeep. Use them through `jobkeep-core` with its
//! `macros` feature.

use proc_macro::TokenStream;
use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::{format_ident, quote};
use syn::parse::{Parse, ParseStream, Parser};
use syn::spanned::Spanned;
use syn::{FnArg, Ident, ItemFn, LitInt, LitStr, Pat, Token, Type, parse_macro_input};

/// Turns an async function into a JobKeep job.
///
/// ```ignore
/// #[job(queue = "emails", max_attempts = 3)]
/// async fn send_email(ctx: &JobContext, to: String, subject: String) -> Result<(), JobError> {
///     // ...
/// }
///
/// client.enqueue(&SendEmail { to, subject }).await?;
/// ```
///
/// Generates a payload struct named after the function in PascalCase, with
/// one public field per argument, and implements `Job` for it. The function
/// itself is kept and can still be called directly.
///
/// - An optional first argument of reference type receives the
///   `&JobContext`.
/// - Every other argument must be a plain `name: Type` whose type is
///   `Serialize + Deserialize + Clone + Debug`; it is cloned for each run.
/// - The function must return `Result<(), E>` where `JobError: From<E>`,
///   such as `JobError` itself or `anyhow::Error`.
///
/// Options: `name = "..."` (the stored job type, default: the function
/// name), `queue = "..."`, `max_attempts = N`.
#[proc_macro_attribute]
pub fn job(attr: TokenStream, item: TokenStream) -> TokenStream {
    let mut options = Options::default();
    let parser = syn::meta::parser(|meta| options.parse(meta));
    parse_macro_input!(attr with parser);
    let func = parse_macro_input!(item as ItemFn);
    expand(options, None, func)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// Like [`#[job]`](macro@job), and also runs the job on a cron schedule.
///
/// ```ignore
/// #[recurring("0 0 3 * * *", queue = "maintenance")]
/// async fn nightly_cleanup(ctx: &JobContext) -> Result<(), JobError> {
///     // ...
/// }
///
/// Server::builder(storage).recurring::<NightlyCleanup>();
/// ```
///
/// The schedule has a leading seconds field and runs in UTC; it is checked
/// at compile time. The function takes no arguments besides the optional
/// context. Implements `Recurring` and `Default` for the generated struct.
#[proc_macro_attribute]
pub fn recurring(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(attr as RecurringArgs);
    let func = parse_macro_input!(item as ItemFn);
    expand(args.options, Some(args.schedule), func)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

#[derive(Default)]
struct Options {
    name: Option<LitStr>,
    queue: Option<LitStr>,
    max_attempts: Option<LitInt>,
}

impl Options {
    fn parse(&mut self, meta: syn::meta::ParseNestedMeta<'_>) -> syn::Result<()> {
        if meta.path.is_ident("name") {
            self.name = Some(meta.value()?.parse()?);
        } else if meta.path.is_ident("queue") {
            self.queue = Some(meta.value()?.parse()?);
        } else if meta.path.is_ident("max_attempts") {
            let lit: LitInt = meta.value()?.parse()?;
            lit.base10_parse::<u32>()?;
            self.max_attempts = Some(lit);
        } else {
            return Err(meta.error("expected `name`, `queue` or `max_attempts`"));
        }
        Ok(())
    }
}

struct RecurringArgs {
    schedule: LitStr,
    options: Options,
}

impl Parse for RecurringArgs {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let schedule: LitStr = input.parse().map_err(|e| {
            syn::Error::new(
                e.span(),
                "expected a cron schedule, e.g. #[recurring(\"0 * * * * *\")]",
            )
        })?;
        if let Err(e) = <cron::Schedule as std::str::FromStr>::from_str(&schedule.value()) {
            return Err(syn::Error::new(
                schedule.span(),
                format!(
                    "invalid cron schedule: {e} (the format is \"sec min hour day month weekday [year]\")"
                ),
            ));
        }
        let mut options = Options::default();
        if input.parse::<Option<Token![,]>>()?.is_some() {
            let rest: TokenStream2 = input.parse()?;
            syn::meta::parser(|meta| options.parse(meta)).parse2(rest)?;
        }
        Ok(Self { schedule, options })
    }
}

fn pascal_case(snake: &str) -> String {
    snake
        .split('_')
        .filter(|part| !part.is_empty())
        .map(|part| {
            let mut chars = part.chars();
            chars
                .next()
                .map(|first| first.to_uppercase().chain(chars).collect::<String>())
                .unwrap_or_default()
        })
        .collect()
}

fn expand(options: Options, schedule: Option<LitStr>, func: ItemFn) -> syn::Result<TokenStream2> {
    let sig = &func.sig;
    if sig.asyncness.is_none() {
        return Err(syn::Error::new(
            sig.fn_token.span(),
            "a job must be an `async fn`",
        ));
    }
    if !sig.generics.params.is_empty() || sig.generics.where_clause.is_some() {
        return Err(syn::Error::new(
            sig.generics.span(),
            "a job function cannot be generic",
        ));
    }

    let mut takes_ctx = false;
    let mut fields: Vec<(Ident, Type)> = Vec::new();
    for (i, input) in sig.inputs.iter().enumerate() {
        let FnArg::Typed(arg) = input else {
            return Err(syn::Error::new(input.span(), "a job cannot take `self`"));
        };
        if i == 0 && matches!(*arg.ty, Type::Reference(_)) {
            takes_ctx = true;
            continue;
        }
        if matches!(*arg.ty, Type::Reference(_)) {
            return Err(syn::Error::new(
                arg.ty.span(),
                "only the first argument (the `&JobContext`) may be a reference; \
                 payload arguments must be owned",
            ));
        }
        let Pat::Ident(pat) = &*arg.pat else {
            return Err(syn::Error::new(arg.pat.span(), "expected `name: Type`"));
        };
        fields.push((pat.ident.clone(), (*arg.ty).clone()));
    }
    if schedule.is_some() && !fields.is_empty() {
        return Err(syn::Error::new(
            fields[0].0.span(),
            "a recurring job takes no arguments besides the `&JobContext`",
        ));
    }

    let fn_name = &sig.ident;
    let vis = &func.vis;
    let struct_name = Ident::new(&pascal_case(&fn_name.to_string()), fn_name.span());
    let job_name = options
        .name
        .unwrap_or_else(|| LitStr::new(&fn_name.to_string(), fn_name.span()));

    let queue_fn = options.queue.map(|queue| {
        quote! {
            fn queue() -> ::jobkeep_core::Queue {
                ::jobkeep_core::Queue::from(#queue)
            }
        }
    });
    let max_attempts_fn = options.max_attempts.map(|n| {
        quote! {
            fn max_attempts() -> u32 {
                #n
            }
        }
    });

    let ctx = if takes_ctx {
        format_ident!("ctx")
    } else {
        format_ident!("_ctx")
    };
    let field_names: Vec<_> = fields.iter().map(|(name, _)| name).collect();
    let field_types: Vec<_> = fields.iter().map(|(_, ty)| ty).collect();
    let ctx_arg = takes_ctx.then(|| quote!(#ctx,));

    let default_derive = schedule
        .is_some()
        .then(|| quote!(::core::default::Default,));
    let recurring_impl = schedule.map(|schedule| {
        quote! {
            impl ::jobkeep_core::Recurring for #struct_name {
                const SCHEDULE: &'static str = #schedule;
            }
        }
    });
    let doc = format!("Job payload for [`{fn_name}`], generated by `#[job]`.");
    let serde_crate = LitStr::new("::jobkeep_core::__private::serde", Span::call_site());

    Ok(quote! {
        #func

        #[doc = #doc]
        #[derive(
            ::core::fmt::Debug,
            ::core::clone::Clone,
            #default_derive
            ::jobkeep_core::__private::serde::Serialize,
            ::jobkeep_core::__private::serde::Deserialize,
        )]
        #[serde(crate = #serde_crate)]
        #vis struct #struct_name {
            #(pub #field_names: #field_types,)*
        }

        impl ::jobkeep_core::Job for #struct_name {
            const NAME: &'static str = #job_name;

            #queue_fn
            #max_attempts_fn

            // `Ok(..?)` converts the function's error type into `JobError`.
            #[allow(clippy::needless_question_mark)]
            async fn run(
                &self,
                #ctx: &::jobkeep_core::JobContext,
            ) -> ::core::result::Result<(), ::jobkeep_core::JobError> {
                ::core::result::Result::Ok(
                    #fn_name(#ctx_arg #(::core::clone::Clone::clone(&self.#field_names)),*).await?,
                )
            }
        }

        #recurring_impl
    })
}

#[cfg(test)]
mod tests {
    use super::pascal_case;

    #[test]
    fn pascal_case_from_snake() {
        assert_eq!(pascal_case("send_email"), "SendEmail");
        assert_eq!(pascal_case("nightly"), "Nightly");
        assert_eq!(pascal_case("_private_job"), "PrivateJob");
        assert_eq!(pascal_case("v2_import"), "V2Import");
    }
}
