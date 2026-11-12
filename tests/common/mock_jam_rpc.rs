#![allow(dead_code)]
//! Minimal mock of the JAM node JSON-RPC WebSocket API, implementing just
//! enough of the subscription handshake for jsonrpsee's ws-client to receive
//! statistics updates.

use futures_util::{SinkExt, StreamExt};
use jam_codec::Encode;
use jam_std_common::{ChainSubUpdate, Statistics};
use jam_types::{AnyBytes, HeaderHash, Slot};
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::Message;

const STATISTICS_SUBSCRIPTION: u64 = 1;
const FINALIZED_SUBSCRIPTION: u64 = 2;

pub struct Block {
    pub header_hash: [u8; 32],
    pub slot: Slot,
    pub statistics: Statistics,
}

pub struct MockJamRpc {
    url: String,
    accept_loop: JoinHandle<()>,
}

impl MockJamRpc {
    /// Every client that connects receives all `blocks` as statistics updates,
    /// in order, and then has its connection closed.
    pub async fn start(blocks: Vec<Block>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let blocks = Arc::new(blocks);
        let accept_loop = tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                tokio::spawn(serve_connection(stream, Arc::clone(&blocks)));
            }
        });
        Self { url, accept_loop }
    }

    pub fn url(&self) -> &str {
        &self.url
    }
}

impl Drop for MockJamRpc {
    fn drop(&mut self) {
        self.accept_loop.abort();
    }
}

async fn serve_connection(stream: TcpStream, blocks: Arc<Vec<Block>>) {
    let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();

    // Closing before both subscriptions are acknowledged fails the client's subscribe call.
    let mut pending = vec!["subscribeStatistics", "subscribeFinalizedBlock"];
    while !pending.is_empty() {
        let Some(message) = ws.next().await else {
            return;
        };
        let Message::Text(text) = message.unwrap() else {
            continue;
        };
        let request: Value = serde_json::from_str(&text).unwrap();
        let method = request["method"].as_str().unwrap();
        let subscription = match method {
            "subscribeStatistics" => STATISTICS_SUBSCRIPTION,
            "subscribeFinalizedBlock" => FINALIZED_SUBSCRIPTION,
            other => panic!("mock JAM RPC got unexpected method {other}"),
        };
        pending.retain(|pending_method| *pending_method != method);
        let ack = json!({ "jsonrpc": "2.0", "result": subscription, "id": request["id"] });
        ws.send(Message::Text(ack.to_string())).await.unwrap();
    }

    for block in blocks.iter() {
        let update = ChainSubUpdate {
            header_hash: HeaderHash(block.header_hash),
            slot: block.slot,
            value: AnyBytes(block.statistics.encode().into()),
        };
        let notification = json!({
            "jsonrpc": "2.0",
            "method": "subscribeStatistics",
            "params": { "subscription": STATISTICS_SUBSCRIPTION, "result": update },
        });
        ws.send(Message::Text(notification.to_string()))
            .await
            .unwrap();
    }

    ws.close(None).await.unwrap();
    // Reading until the client hangs up keeps its unsubscribe requests from
    // turning our close into a TCP reset that could drop unread notifications.
    while let Some(Ok(_)) = ws.next().await {}
}
