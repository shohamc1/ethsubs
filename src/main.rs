use futures::{join, Stream, StreamExt};
use jsonrpsee::{
    core::{
        client::{Subscription, SubscriptionClientT},
    },
    rpc_params,
    server::ServerBuilder,
    ws_client::WsClientBuilder,
    RpcModule, PendingSubscriptionSink, SubscriptionMessage, TrySendError
};
use serde::Serialize;
use std::net::SocketAddr;
use std::time::Duration;
use serde_json::Value;
use tokio::sync::broadcast;
use tokio::time::interval;
use tokio_stream::wrappers::{BroadcastStream, IntervalStream};

#[tokio::main]
async fn main() {
    let server = ServerBuilder::default()
        .build("127.0.0.1:0".parse::<SocketAddr>().unwrap())
        .await
        .unwrap();
    let (tx, _rx) = broadcast::channel::<Value>(16);

    let mut module = RpcModule::new(tx.clone());

    module
        .register_subscription("sub_ping", "notif_ping", "unsub_ping", |_, mut pending, ctx| async move {
            let rx = ctx.subscribe();
            let stream = BroadcastStream::new(rx);
            pipe_from_stream_and_drop(pending, stream).await.map_err::<anyhow::Error, _>(Into::into);
        })
        .unwrap();

    let addr = server.local_addr().unwrap();

    let handle = server.start(module).unwrap();

    tokio::spawn(handle.stopped());

    let url = format!("ws://{}", addr);
    let client = WsClientBuilder::default().build(&url).await.unwrap();

    let mut ping_sub: Subscription<Value> = client
        .subscribe("sub_ping", rpc_params!(), "unsub_ping")
        .await
        .unwrap();

    tokio::spawn(send_items(tx));

    for _ in 0..10 {
        let ping = ping_sub.next().await.unwrap().unwrap();
        println!("ping: {:?}", ping);
    }
}

pub async fn pipe_from_stream_and_drop(
    pending: PendingSubscriptionSink,
    mut stream: BroadcastStream<Value>
) -> Result<(), anyhow::Error> {
    let mut sink = pending.accept().await?;

    loop {
        tokio::select! {
			_ = sink.closed() => return Err(anyhow::anyhow!("Subscription was closed")),
			maybe_item = stream.next() => {
				match maybe_item {
                    Some(Err(e)) => {
                        println!("e : {:#?}", e);
                    },
					None => return Err(anyhow::anyhow!("Subscription executed successful")),
					Some(item) => {
                    let msg = SubscriptionMessage::from_json(&item.unwrap())?;

                    match sink.try_send(msg) {
                        Ok(_) => (),
                        Err(TrySendError::Closed(_)) => return Err(anyhow::anyhow!("Subscription executed successful")),
                        // channel is full, let's be naive an just drop the message.
                        Err(TrySendError::Full(_)) => (),
                    }
                }
                }
            }
        }
    }
}

async fn send_items(tx: broadcast::Sender<Value>) {
    let data = r#"
        {
            "hello": "there"
        }"#;

    for c in 1..=100 {
        let v = serde_json::from_str(data).unwrap();
        let _ = tx.send(v);
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
}