use std::time::Duration;

use crate::core::{manager::TaggedReceiver, tag::TagId, types::Record};
use futures::StreamExt;
use log::{debug, warn};
use miette::Diagnostic;
use thiserror::Error;
use tokio::sync::broadcast::error::RecvError;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Error, Diagnostic)]
pub enum Error {
    #[error("Channel {0} has been closed")]
    ChannelClosed(TagId),
    #[error("Timeout")]
    Timeout,
    #[error("Canceled")]
    Canceled,
}

pub async fn recv(
    who: &TagId,
    inbounds: &mut [TaggedReceiver],
    timeout: Duration,
    ctx: CancellationToken,
) -> Result<Record, Error> {
    let now = std::time::Instant::now();
    let mut time_left = timeout;

    let tags = inbounds
        .iter()
        .map(|inbound| inbound.tag().clone())
        .collect::<Vec<_>>();

    'body: loop {
        let futs = inbounds.iter_mut().map(|inbound| {
            let fut = async move { (inbound.tag().clone(), inbound.recv().await) };

            Box::pin(fut)
        });

        tokio::select! {
            (record, i, _) = futures::future::select_all(futs) => match record {
                (_, Ok(record)) => {
                  debug!("{} received 1 record from {}", who, tags[i]);
                  return Ok(record);
                },
                (tag, Err(RecvError::Closed)) => {
                    return Err(Error::ChannelClosed(tag))
                },
                (tag, Err(RecvError::Lagged(n))) => {
                    warn!("{}: inbound lagged additional {}", tag, n);
                    time_left = timeout.saturating_sub(now.elapsed());
                    continue 'body;
                }
            },
            _ = tokio::time::sleep(time_left) => {
                return Err(Error::Timeout);
            }
            _ = ctx.cancelled() => {
                return Err(Error::Canceled);
            }
        }
    }
}

pub async fn recv_batch(
    who: &TagId,
    inbounds: &mut [TaggedReceiver],
    timeout: Duration,
    num_records: usize,
    ctx: CancellationToken,
) -> Result<Vec<Record>, Error> {
    let now = std::time::Instant::now();
    let mut time_left = timeout;

    let tags = inbounds
        .iter()
        .map(|inbound| inbound.tag().clone())
        .collect::<Vec<_>>();

    let mut records = Vec::new();

    'body: loop {
        let futs = inbounds.iter_mut().map(|inbound| {
            let fut = async move { (inbound.tag().clone(), inbound.recv().await) };

            Box::pin(fut)
        });

        tokio::select! {
            (record, i, _) = futures::future::select_all(futs) => match record {
                (_, Ok(record)) => {
                  time_left = timeout.saturating_sub(now.elapsed());

                  records.push(record);
                  if records.len() >= num_records {
                      debug!("{} received {} record from {}", who, records.len(), tags[i]);
                      return Ok(records);
                  }
                },
                (tag, Err(RecvError::Closed)) => {
                    return Err(Error::ChannelClosed(tag))
                },
                (tag, Err(RecvError::Lagged(n))) => {
                    warn!("{}: inbound lagged additional {}", tag, n);
                    time_left = timeout.saturating_sub(now.elapsed());
                    continue 'body;
                }
            },
            _ = tokio::time::sleep(time_left) => match records.len() {
                0 => return Err(Error::Timeout),
                _ => return Ok(records),
            },
            _ = ctx.cancelled() => {
                return Err(Error::Canceled);
            }
        }
    }
}
