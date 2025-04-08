use async_trait::async_trait;
use log::{error, info};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::tag::HasTag;

mod error;

pub use error::Error;

#[async_trait]
pub trait Actor: HasTag + Send + 'static {
    type Error: Send + 'static + Into<error::Error>;
    async fn poll(&mut self, ctx: CancellationToken) -> Result<(), Self::Error>;

    fn is_blocking(&self) -> bool {
        false
    }
}

pub fn spawn<T, Error>(actor: Box<T>, ctx: CancellationToken) -> JoinHandle<()>
where
    T: Actor<Error = Error> + Send + 'static + ?Sized,
    Error: Send + 'static + Into<error::Error>,
{
    let is_blocking = actor.is_blocking();
    let tag = actor.tag().clone();

    let name = format!(
        "{}: {}",
        tag,
        if is_blocking { "blocking" } else { "async" }
    );

    let mut actor = actor;

    match is_blocking {
        true => todo!("Blocking actor not supported yet"),
        false => tokio::task::Builder::new()
            .name(&name)
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
                                error!("{}: error: {}", tag, err.into());
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
            .expect("Failed to spawn actor"),
    }
}
