use futures::{AsyncBufReadExt, TryStreamExt};
use k8s_openapi::api::core::v1::{Namespace, Pod};
use kube::{
    api::{Api, ListParams, LogParams, ResourceExt},
    Client,
};
use tokio::time::sleep;

#[derive(Clone, Debug)]
struct LogIdentifier {
    namespace: String,
    pod: String,
    container: Option<String>,
}

struct LogStream {
    id: LogIdentifier,
}

impl LogStream {
    fn new(client: Client, id: LogIdentifier) -> Self {
        let pods: Api<Pod> = Api::namespaced(client, &*id.namespace);
        let podname = id.pod.clone();
        tokio::spawn(async move {
            let mut log_params = LogParams::default();
            log_params.follow = true;
            log_params.since_seconds = Some(15);
            if let Ok(logs) = pods.log_stream(&podname, &log_params).await {
                let mut lines = logs.lines();
                while let Ok(Some(line)) = lines.try_next().await {
                    println!("{}", line);
                }
            }
            println!("Done");
        });
        Self { id }
    }
}

struct LogStreamManager {
    client: Client,
}

impl LogStreamManager {
    fn new(client: Client) -> Self {
        Self { client }
    }

    fn add_stream(&self, id: LogIdentifier) {
        LogStream::new(self.client.clone(), id);
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Infer the runtime environment and try to create a Kubernetes Client
    println!("try client");
    let client = Client::try_default().await?;
    println!("got client");

    let logstreams = LogStreamManager::new(client.clone());
    // Read pods in the configured namespace into the typed interface from k8s-openapi
    let namespaces: Api<Namespace> = Api::all(client.clone());
    if let Ok(namespaces_list) = namespaces.list(&ListParams::default()).await {
        for namespace in namespaces_list.iter() {
            println!("{:?}", namespace.name_any());
            let pods: Api<Pod> = Api::namespaced(client.clone(), &namespace.name_any());
            for pod in pods.list(&ListParams::default()).await.unwrap() {
                let id = LogIdentifier {
                    namespace: namespace.name_any(),
                    pod: pod.name_any(),
                    container: None,
                };
                println!("{:?}", id);
                logstreams.add_stream(id);
            }
        }
    }
    loop {
        sleep(core::time::Duration::new(60, 0)).await;
    }
    Ok(())
}
