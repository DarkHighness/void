use async_trait::async_trait;
use log::{error, info};
use miette::Diagnostic;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::tag::HasTag;

mod error;

pub use error::Error;

#[async_trait]
pub trait Actor: HasTag + Send + 'static {
    type Error: Send + Sync + Diagnostic + 'static;
    async fn poll(&mut self, ctx: CancellationToken) -> miette::Result<(), Self::Error>;
}

pub fn spawn<T, Error>(actor: Box<T>, ctx: CancellationToken) -> JoinHandle<()>
where
    T: Actor<Error = Error> + Send + ?Sized + 'static,
    Error: Send + Sync + Diagnostic + 'static,
{
    let tag = actor.tag().clone();

    let mut actor = actor;

    tokio::task::Builder::new()
        .name(&tag.to_string())
        .spawn(async move {
            let actor = actor.as_mut();

            loop {
                let poll_start = std::time::Instant::now();

                tokio::select! {
                    _ = ctx.cancelled() => {
                        info!("{}: cancelled", tag);
                        return;
                    }
                    r = actor.poll(ctx.clone()) => match r {
                        Ok(()) => {}
                        Err(err) => {
                            let report = miette::Report::new(err);
                            error!("{}: error: {:?}", tag, report);
                        },
                    }
                }

                let poll_elapsed = poll_start.elapsed();
                if poll_elapsed > std::time::Duration::from_millis(200) {
                    info!("{}: poll took {:?}", tag, poll_elapsed);
                }

                // Yield to allow other tasks to run
                tokio::task::yield_now().await;
            }
        })
        .expect("Failed to spawn actor")
}
