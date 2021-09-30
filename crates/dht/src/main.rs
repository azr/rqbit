use std::{str::FromStr, time::Duration};

use anyhow::Context;
use dht::{Dht, Id20};
use log::info;
use tokio::io::AsyncWriteExt;
use tokio_stream::StreamExt;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    pretty_env_logger::init();

    let info_hash = Id20::from_str("64a980abe6e448226bb930ba061592e44c3781a1").unwrap();
    let dht = Dht::new().await.context("error initializing DHT")?;
    let mut stream = dht.get_peers(info_hash).await?;

    let stats_printer = async {
        loop {
            tokio::time::sleep(Duration::from_secs(5)).await;
            info!("DHT stats: {:?}", dht.stats().await);
        }
        #[allow(unreachable_code)]
        Ok::<_, anyhow::Error>(())
    };

    let routing_table_dumper = async {
        loop {
            tokio::time::sleep(Duration::from_secs(15)).await;
            dht.with_routing_table(|r| async move {
                let filename = "/tmp/routing-table.json";
                let mut f = tokio::fs::OpenOptions::new()
                    .create(true)
                    .write(true)
                    .open(filename)
                    .await
                    .unwrap();
                f.write_all(serde_json::to_string_pretty(&r).unwrap().as_bytes())
                    .await
                    .unwrap();
                info!("Dumped DHT routing table to {}", filename);
            }).await;
        }
        #[allow(unreachable_code)]
        Ok::<_, anyhow::Error>(())
    };

    let peer_printer = async {
        while let Some(peer) = stream.next().await {
            log::info!("peer found: {}", peer)
        }
        Ok(())
    };

    let res = tokio::select! {
        res = stats_printer => res,
        res = peer_printer => res,
        res = routing_table_dumper => res,
    };
    res
}
