use tracing::Level;
use tracing_subscriber::EnvFilter;

pub fn init_tracing_base() -> anyhow::Result<()> {
    let filter = EnvFilter::from_default_env().add_directive(Level::DEBUG.into());
    let subscriber = tracing_subscriber::fmt()
        .with_env_filter(filter)
        // Use a more compact, abbreviated log format
        .compact()
        // Display source code file paths
        .with_file(true)
        // Display source code line numbers
        .with_line_number(true)
        .with_thread_ids(true)
        // Don't display the event's target (module path)
        .with_target(true)
        // Build the subscriber
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;
    Ok(())
}
