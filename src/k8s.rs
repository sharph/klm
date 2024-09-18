use futures::{AsyncBufReadExt, TryStreamExt};
use k8s_openapi::api::core::v1::Pod;
use kube::{
    api::{Api, LogParams, ResourceExt},
    runtime::{watcher, WatchStreamExt},
    Client,
};
use std::{cmp::Ordering, collections::HashMap, error::Error};
use thiserror::Error;
use tokio::select;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct LogIdentifier {
    pub namespace: String,
    pub pod: String,
    pub container: Option<String>,
}

impl LogIdentifier {
    fn new(namespace: String, pod: String, container: Option<String>) -> Self {
        Self {
            namespace,
            pod,
            container,
        }
    }
}

#[derive(Error, Debug)]
enum LogLineError {
    #[error("no timestamp")]
    NoTimestamp,
}

#[derive(Debug, Eq, PartialEq)]
pub struct LogLine {
    pub id: LogIdentifier,
    pub message: String,
    pub timestamp: i128,
}

impl Ord for LogLine {
    fn cmp(&self, other: &Self) -> Ordering {
        self.timestamp.cmp(&other.timestamp)
    }
}

impl PartialOrd for LogLine {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl LogLine {
    fn new(id: LogIdentifier, message: String) -> Result<Self, Box<dyn Error>> {
        if let Some((timestamp, message)) = message.split_once(" ") {
            let timestamp = time::OffsetDateTime::parse(
                timestamp,
                &time::format_description::well_known::Rfc3339,
            )?
            .unix_timestamp_nanos();
            Ok(Self {
                id,
                message: message.to_string(),
                timestamp,
            })
        } else {
            Err(Box::new(LogLineError::NoTimestamp))
        }
    }
}

pub struct LogStream {
    id: LogIdentifier,
    cancellation_token: CancellationToken,
}

impl LogStream {
    fn new(
        client: Client,
        id: LogIdentifier,
        tx: tokio::sync::mpsc::UnboundedSender<LogLine>,
    ) -> Self {
        let pods: Api<Pod> = Api::namespaced(client, &id.namespace);
        let podname = id.pod.clone();
        let id2 = id.clone();
        let cancellation_token = CancellationToken::new();
        let cloned_token = cancellation_token.clone();
        tokio::spawn(async move {
            let mut log_params = LogParams::default();
            log_params.follow = true;
            log_params.since_seconds = Some(600);
            log_params.timestamps = true;
            if let Ok(logs) = pods.log_stream(&podname, &log_params).await {
                let mut lines = logs.lines();
                loop {
                    select! {
                        _ = cloned_token.cancelled() => {
                            break;
                        }
                        line = lines.try_next() => {
                            if let Ok(Some(line)) = line {
                                let _ = tx.send(LogLine::new(id2.clone(), line).expect("couldn't create log line"));
                            }
                        }
                    }
                }
            }
        });
        Self {
            id,
            cancellation_token,
        }
    }
}

#[derive(Error, Debug)]
enum LogStreamManagerError {
    #[error("stream not found")]
    StreamNotFound,
}

#[derive(Debug)]
pub enum LogStreamManagerMessage {
    Log(LogLine),
    LogSourceCreated(LogIdentifier),
    LogSourceRemoved(LogIdentifier),
    LogSourceCancelled(LogIdentifier),
}

pub struct LogStreamManager {
    client: Client,
    tx: tokio::sync::mpsc::UnboundedSender<LogLine>,
    outbound_tx: tokio::sync::mpsc::Sender<LogStreamManagerMessage>,
    pub streams: HashMap<LogIdentifier, LogStream>,
}

impl LogStreamManager {
    pub async fn new(
        outbound_tx: tokio::sync::mpsc::Sender<LogStreamManagerMessage>,
    ) -> Result<Self, Box<dyn Error>> {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let outbound_tx2 = outbound_tx.clone();
        tokio::spawn(async move {
            while let Some(message) = rx.recv().await {
                let _ = outbound_tx2
                    .send(LogStreamManagerMessage::Log(message))
                    .await;
            }
        });
        let client = Client::try_default().await?;
        let pods: Api<Pod> = Api::all(client.clone());
        let outbound_tx2 = outbound_tx.clone();
        tokio::spawn(async move {
            watcher(pods.clone(), watcher::Config::default())
                .applied_objects()
                .try_for_each(|p| async {
                    let namespace = p.namespace().unwrap_or("".to_string());
                    let pod = p.name_any();
                    for container in p.spec.unwrap().containers.into_iter() {
                        let _ = outbound_tx2
                            .send(LogStreamManagerMessage::LogSourceCreated(
                                LogIdentifier::new(
                                    namespace.clone(),
                                    pod.clone(),
                                    Some(container.name.to_string()),
                                ),
                            ))
                            .await;
                    }
                    Ok(())
                })
                .await
                .expect("watcher failed");
        });
        Ok(Self {
            client,
            tx,
            outbound_tx,
            streams: HashMap::new(),
        })
    }

    pub fn add_stream(&mut self, id: LogIdentifier) {
        self.streams.insert(
            id.clone(),
            LogStream::new(self.client.clone(), id, self.tx.clone()),
        );
    }

    pub fn drop_stream(&mut self, id: &LogIdentifier) -> Result<(), Box<dyn Error>> {
        if let Some(stream) = self.streams.remove(id) {
            stream.cancellation_token.cancel();
            let tx = self.outbound_tx.clone();
            let id = id.clone();
            tokio::spawn(async move {
                let _ = tx
                    .send(LogStreamManagerMessage::LogSourceCancelled(id))
                    .await;
            });
            return Ok(());
        }
        Err(Box::new(LogStreamManagerError::StreamNotFound))
    }
}
