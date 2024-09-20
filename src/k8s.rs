use futures::{AsyncBufReadExt, TryStreamExt};
use k8s_openapi::api::core::v1::{Pod, PodStatus};
use kube::{
    api::{Api, LogParams, ResourceExt},
    runtime::{watcher, WatchStreamExt},
    Client,
};
use std::collections::BTreeSet;
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

impl Ord for LogIdentifier {
    fn cmp(&self, other: &Self) -> Ordering {
        let o = self.namespace.cmp(&other.namespace);
        if o != Ordering::Equal {
            return o;
        }
        let o = self.pod.cmp(&other.pod);
        if o != Ordering::Equal {
            return o;
        }
        self.container.cmp(&other.container)
    }
}

impl PartialOrd for LogIdentifier {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(&other))
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

enum LogStreamMessage {
    Started(LogIdentifier),
    Ended(LogIdentifier),
    Log(LogLine),
}

impl LogStream {
    fn new(
        client: Client,
        id: LogIdentifier,
        tx: tokio::sync::mpsc::UnboundedSender<LogStreamMessage>,
    ) -> Self {
        let pods: Api<Pod> = Api::namespaced(client, &id.namespace);
        let podname = id.pod.clone();
        let id2 = id.clone();
        let cancellation_token = CancellationToken::new();
        let cloned_token = cancellation_token.clone();
        tokio::spawn(async move {
            let mut last = 0;
            let mut log_params = LogParams::default();
            log_params.follow = true;
            log_params.since_seconds = Some(600);
            log_params.timestamps = true;
            let _ = tx.send(LogStreamMessage::Started(id2.clone()));
            while let Ok(logs) = pods.log_stream(&podname, &log_params).await {
                let mut lines = logs.lines();
                loop {
                    select! {
                        _ = cloned_token.cancelled() => {
                            return;
                        }
                        line = lines.try_next() => {
                            if let Ok(Some(line)) = line {
                                let log_line = LogLine::new(id2.clone(), line).expect("couldn't create log line");
                                if log_line.timestamp > last {
                                    last = log_line.timestamp;
                                    let _ = tx.send(LogStreamMessage::Log(
                                        log_line
                                    ));
                                }
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
    LogSourceUpdated(LogIdentifier),
    LogSourceRemoved(LogIdentifier),
    LogSourceSubscribed(LogIdentifier),
    LogSourceCancelled(LogIdentifier),
}

pub struct LogStreamManager {
    client: Client,
    tx: tokio::sync::mpsc::UnboundedSender<LogStreamMessage>,
    outbound_tx: tokio::sync::mpsc::Sender<LogStreamManagerMessage>,
    pub streams: HashMap<LogIdentifier, LogStream>,
    cancellation_token: CancellationToken,
}

impl LogStreamManager {
    pub async fn new(
        outbound_tx: tokio::sync::mpsc::Sender<LogStreamManagerMessage>,
    ) -> Result<Self, Box<dyn Error>> {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let outbound_tx2 = outbound_tx.clone();
        let cancellation_token = CancellationToken::new();
        let cancellation_token2 = cancellation_token.clone();
        tokio::spawn(async move {
            loop {
                let msg;
                select! {
                    _ = cancellation_token2.cancelled() => {
                        rx.close();
                        break;
                    }
                    message = rx.recv() => {
                        match message {
                            Some(m) => msg = m,
                            _ => continue,
                        }
                    }
                }
                match msg {
                    LogStreamMessage::Log(m) => {
                        let _ = outbound_tx2.send(LogStreamManagerMessage::Log(m)).await;
                    }
                    LogStreamMessage::Started(id) => {
                        let _ = outbound_tx2
                            .send(LogStreamManagerMessage::LogSourceSubscribed(id))
                            .await;
                    }
                    LogStreamMessage::Ended(id) => {
                        let _ = outbound_tx2
                            .send(LogStreamManagerMessage::LogSourceCancelled(id))
                            .await;
                    }
                }
            }
        });
        let client = Client::try_default().await?;
        let pods: Api<Pod> = Api::all(client.clone());
        let outbound_tx2 = outbound_tx.clone();
        tokio::spawn(async move {
            watcher(pods.clone(), watcher::Config::default())
                .touched_objects()
                .try_for_each(|p| async {
                    let namespace = p.namespace().unwrap_or("".to_string());
                    let pod = p.name_any();
                    for container in p.spec.unwrap().containers.into_iter() {
                        let log_id = LogIdentifier::new(
                            namespace.clone(),
                            pod.clone(),
                            Some(container.name.to_string()),
                        );
                        if ["Succeeded", "Failed"].iter().any(|s| {
                            p.status.as_ref().map(|s| s.phase.clone()) == Some(Some(s.to_string()))
                        }) {
                            let _ = outbound_tx2
                                .send(LogStreamManagerMessage::LogSourceRemoved(log_id))
                                .await;
                        } else {
                            let _ = outbound_tx2
                                .send(LogStreamManagerMessage::LogSourceUpdated(log_id))
                                .await;
                        }
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
            cancellation_token,
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

    pub fn close(&mut self) -> Result<(), Box<dyn Error>> {
        let streams: Vec<_> = self.streams.keys().map(LogIdentifier::clone).collect();
        for stream in streams {
            self.drop_stream(&stream)?;
        }
        Ok(())
    }
}
