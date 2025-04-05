use async_trait::async_trait;
use log::info;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use super::tag::HasTag;

mod error;

pub use error::Error;

#[async_trait]
pub trait Actor: HasTag + Send + 'static {
    type Error: Send + 'static + Into<error::Error>;
    async fn poll(&mut self, ctx: CancellationToken) -> Result<(), Self::Error>;
}

pub fn spawn<T, Error>(
    actor: Box<T>,
    ctx: CancellationToken,
) -> JoinHandle<std::result::Result<(), error::Error>>
where
    T: Actor<Error = Error> + Send + 'static + ?Sized,
    Error: Send + 'static + Into<error::Error>,
{
    let mut actor = actor;
    let tag = actor.tag().clone();

    tokio::task::Builder::new()
        .name(&tag.to_string())
        .spawn(async move {
            let actor = actor.as_mut();

            loop {
                tokio::select! {
                    _ = ctx.cancelled() => {
                        info!("{}: cancelled", tag);
                        return Ok(());
                    }
                    r = actor.poll(ctx.clone()) => match r {
                        Ok(()) => {}
                        Err(err) => return Err(err.into()),
                    }
                }
            }
        })
        .expect("Failed to spawn actor")
}
