use futures::{future::{Either, self}, StreamExt};
use jsonrpsee::{
    core::{
        client::{Subscription, SubscriptionClientT},
    },
    rpc_params,
    server::ServerBuilder,
    ws_client::WsClientBuilder,
    RpcModule, PendingSubscriptionSink, SubscriptionMessage
};
use serde_json::Value;
use std::net::SocketAddr;
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;

const NUM_SUBSCRIPTION_RESPONSES: usize = 5;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::FmtSubscriber::builder()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init()
        .expect("setting default subscriber failed");

    let addr = run_server().await?;
    let url = format!("ws://{}", addr);

    let client1 = WsClientBuilder::default().build(&url).await?;
    let client2 = WsClientBuilder::default().build(&url).await?;
    let sub1: Subscription<Value> = client1.subscribe("subscribe_hello", rpc_params![], "unsubscribe_hello").await?;
    let sub2: Subscription<Value> = client2.subscribe("subscribe_hello", rpc_params![], "unsubscribe_hello").await?;

    let fut1 = sub1.take(NUM_SUBSCRIPTION_RESPONSES).for_each(|r| async move { tracing::info!("sub1 rx: {:?}", r) });
    let fut2 = sub2.take(NUM_SUBSCRIPTION_RESPONSES).for_each(|r| async move { tracing::info!("sub2 rx: {:?}", r) });

    future::join(fut1, fut2).await;

    Ok(())
}

async fn run_server() -> anyhow::Result<SocketAddr> {
    // let's configure the server only hold 5 messages in memory.
    let server = ServerBuilder::default().set_message_buffer_capacity(5).build("127.0.0.1:0").await?;
    let (tx, _rx) = broadcast::channel::<Value>(16);

    let mut module = RpcModule::new(tx.clone());

    std::thread::spawn(move || produce_items(tx));

    module
        .register_subscription("subscribe_hello", "s_hello", "unsubscribe_hello", |_, pending, tx| async move {
            let rx = tx.subscribe();
            let stream = BroadcastStream::new(rx);
            pipe_from_stream_with_bounded_buffer(pending, stream).await?;
            Ok(())
        })
        .unwrap();
    let addr = server.local_addr()?;
    let handle = server.start(module)?;

    // In this example we don't care about doing shutdown so let's it run forever.
    // You may use the `ServerHandle` to shut it down or manage it yourself.
    tokio::spawn(handle.stopped());

    Ok(addr)
}

async fn pipe_from_stream_with_bounded_buffer(
    pending: PendingSubscriptionSink,
    stream: BroadcastStream<Value>,
) -> Result<(), anyhow::Error> {
    let sink = pending.accept().await?;
    let closed = sink.closed();

    futures::pin_mut!(closed, stream);

    loop {
        match future::select(closed, stream.next()).await {
            // subscription closed.
            Either::Left((_, _)) => break Ok(()),

            // received new item from the stream.
            Either::Right((Some(Ok(item)), c)) => {
                let notif = SubscriptionMessage::from_json(&item)?;

                // NOTE: this will block until there a spot in the queue
                // and you might want to do something smarter if it's
                // critical that "the most recent item" must be sent when it is produced.
                if sink.send(notif).await.is_err() {
                    break Ok(());
                }

                closed = c;
            }

            // Send back back the error.
            Either::Right((Some(Err(e)), _)) => break Err(e.into()),

            // Stream is closed.
            Either::Right((None, _)) => break Ok(()),
        }
    }
}

// Naive example that broadcasts the produced values to all active subscribers.
fn produce_items(tx: broadcast::Sender<Value>) {
    for _ in 1..=100 {
        std::thread::sleep(std::time::Duration::from_millis(1));

        let _ = tx.send(Value::String("asdf".to_owned()));
    }
}
