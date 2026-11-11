//! `jobkeep`: manage a JobKeep Postgres database from the command line.
//!
//! ```text
//! jobkeep migrate
//! jobkeep stats
//! jobkeep list --state failed --limit 20
//! jobkeep show <id>
//! jobkeep requeue <id>
//! jobkeep delete <id>
//! jobkeep purge --older-than 7d
//! jobkeep dashboard --bind 127.0.0.1:8080
//! ```
//!
//! The database comes from `--database-url` or `DATABASE_URL`.

#[cfg(not(feature = "postgres"))]
compile_error!("jobkeep-cli needs the `postgres` feature");

use std::process::ExitCode;

use anyhow::{Context, bail};
use chrono::{TimeDelta, Utc};
use clap::{Parser, Subcommand};
use jobkeep_client::Client;
use jobkeep_core::{JobFilter, JobId, JobRecord, State};
use jobkeep_postgres::PgStorage;

#[derive(Debug, Parser)]
#[command(name = "jobkeep", version, about = "Manage JobKeep background jobs")]
struct Cli {
    /// Postgres connection string.
    #[arg(long, env = "DATABASE_URL", global = true, hide_env_values = true)]
    database_url: Option<String>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Create or upgrade the job table.
    Migrate,
    /// Show job counts by state.
    Stats,
    /// List jobs, newest first.
    List {
        #[arg(long)]
        state: Option<State>,
        #[arg(long)]
        queue: Option<String>,
        #[arg(long = "type")]
        job_type: Option<String>,
        #[arg(long, default_value_t = 20)]
        limit: u32,
        #[arg(long, default_value_t = 0)]
        offset: u32,
    },
    /// Show one job as JSON.
    Show { id: JobId },
    /// Run a succeeded, failed or deleted job again.
    Requeue { id: JobId },
    /// Delete a job (and continuations waiting on it).
    Delete { id: JobId },
    /// Permanently remove succeeded and deleted jobs older than a duration.
    Purge {
        /// e.g. `30m`, `12h`, `7d`.
        #[arg(long, value_parser = parse_duration)]
        older_than: TimeDelta,
    },
    /// Serve the web dashboard.
    #[cfg(feature = "dashboard")]
    Dashboard {
        #[arg(long, default_value = "127.0.0.1:8080")]
        bind: std::net::SocketAddr,
    },
}

fn parse_duration(s: &str) -> Result<TimeDelta, String> {
    let (digits, unit) = s.split_at(s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len()));
    let n: i64 = digits
        .parse()
        .map_err(|_| format!("expected a number followed by s, m, h or d, got {s:?}"))?;
    match unit {
        "s" => Ok(TimeDelta::seconds(n)),
        "m" => Ok(TimeDelta::minutes(n)),
        "h" => Ok(TimeDelta::hours(n)),
        "d" => Ok(TimeDelta::days(n)),
        _ => Err(format!("unknown unit {unit:?} in {s:?}; use s, m, h or d")),
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "jobkeep=info,jobkeep_dashboard=info,warn".into()),
        )
        .with_writer(std::io::stderr)
        .init();

    match run(Cli::parse()).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e:#}");
            ExitCode::FAILURE
        }
    }
}

async fn run(cli: Cli) -> anyhow::Result<()> {
    let Some(url) = cli.database_url else {
        bail!("no database: pass --database-url or set DATABASE_URL");
    };
    let storage = PgStorage::connect(&url)
        .await
        .context("connecting to the database")?;

    if let Command::Migrate = cli.command {
        storage.migrate().await.context("running migrations")?;
        println!("migrations applied");
        return Ok(());
    }

    // Every other command needs the table; say so plainly instead of failing
    // with a SQL error (or, for the dashboard, on every page load).
    let table: Option<String> = sqlx::query_scalar("SELECT to_regclass('jobkeep_jobs')::text")
        .fetch_one(storage.pool())
        .await
        .context("checking for the jobkeep_jobs table")?;
    if table.is_none() {
        bail!("this database has no jobkeep_jobs table yet; run `jobkeep migrate` first");
    }

    let client = Client::new(storage);
    match cli.command {
        Command::Migrate => unreachable!("handled above"),
        Command::Stats => {
            let stats = client.stats().await?;
            for state in State::ALL {
                println!("{:<11} {:>8}", state.as_str(), stats.count(state));
            }
            println!("{:<11} {:>8}", "total", stats.total());
        }
        Command::List {
            state,
            queue,
            job_type,
            limit,
            offset,
        } => {
            let mut filter = JobFilter::new().limit(limit).offset(offset);
            filter.state = state;
            filter.queue = queue.map(Into::into);
            filter.job_type = job_type;
            print_table(&client.list(&filter).await?);
        }
        Command::Show { id } => match client.get(id).await? {
            Some(job) => println!("{}", serde_json::to_string_pretty(&job)?),
            None => bail!("job {id} not found"),
        },
        Command::Requeue { id } => {
            client.requeue(id).await?;
            println!("requeued {id}");
        }
        Command::Delete { id } => {
            client.delete(id).await?;
            println!("deleted {id}");
        }
        Command::Purge { older_than } => {
            let n = client.purge(Utc::now() - older_than).await?;
            println!("purged {n} jobs");
        }
        #[cfg(feature = "dashboard")]
        Command::Dashboard { bind } => {
            let shutdown = async {
                tokio::signal::ctrl_c().await.ok();
            };
            jobkeep_dashboard::serve(client.storage().clone(), bind, shutdown)
                .await
                .with_context(|| format!("serving dashboard on {bind}"))?;
        }
    }
    Ok(())
}

fn print_table(jobs: &[JobRecord]) {
    if jobs.is_empty() {
        println!("no jobs");
        return;
    }
    println!(
        "{:<36}  {:<20}  {:<12}  {:<10}  {:>7}  {:<20}  last error",
        "id", "type", "queue", "state", "attempt", "updated"
    );
    for job in jobs {
        let error = job.last_error.as_deref().unwrap_or("");
        let error: String = error.chars().take(60).collect();
        println!(
            "{:<36}  {:<20}  {:<12}  {:<10}  {:>7}  {:<20}  {}",
            job.id,
            job.job_type,
            job.queue,
            job.state,
            format!("{}/{}", job.attempt, job.max_attempts),
            job.updated_at.format("%Y-%m-%d %H:%M:%S"),
            error
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_durations() {
        assert_eq!(parse_duration("45s"), Ok(TimeDelta::seconds(45)));
        assert_eq!(parse_duration("30m"), Ok(TimeDelta::minutes(30)));
        assert_eq!(parse_duration("12h"), Ok(TimeDelta::hours(12)));
        assert_eq!(parse_duration("7d"), Ok(TimeDelta::days(7)));
        assert!(parse_duration("7").is_err());
        assert!(parse_duration("7w").is_err());
        assert!(parse_duration("d").is_err());
    }

    #[test]
    fn cli_definition_is_valid() {
        use clap::CommandFactory;
        Cli::command().debug_assert();
    }

    #[test]
    fn parses_list_filters() {
        let cli = Cli::try_parse_from(["jobkeep", "list", "--state", "failed", "--type", "email"])
            .unwrap();
        let Command::List {
            state, job_type, ..
        } = cli.command
        else {
            panic!("expected list");
        };
        assert_eq!(state, Some(State::Failed));
        assert_eq!(job_type.as_deref(), Some("email"));
        assert!(Cli::try_parse_from(["jobkeep", "list", "--state", "nope"]).is_err());
    }
}
