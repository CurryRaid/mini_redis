use std::{
    io::{self, BufRead},
    net::SocketAddr,
    // string,
};

use lazy_static::lazy_static;
use my_redis::{FilterLayer, LogLayer};
use pilota::FastStr;
// use tracing_subscriber::layer::Filter;
use volo_gen::volo::redis::{GetItemResponse, RedisCommand};
lazy_static! {
    static ref CLIENT: volo_gen::volo::redis::ItemServiceClient = {
        let addr: SocketAddr = "127.0.0.1:1234".parse().unwrap();
        volo_gen::volo::redis::ItemServiceClientBuilder::new("my_redis")
            .layer_outer(LogLayer)
            .layer_outer(FilterLayer)
            .address(addr)
            .build()
    };
}
async fn handle(request: &str) {
    let args = request.split_whitespace().collect::<Vec<&str>>();
    let cmd = args[0];
    let args = &args[1..];
    let req = volo_gen::volo::redis::GetItemRequest {
        cmd: match cmd {
            "Get" => RedisCommand::Get,
            "Set" => RedisCommand::Set,
            "Ping" => RedisCommand::Ping,
            "Del" => RedisCommand::Del,
            _ => RedisCommand::Unkonwn,
        },
        args: Some(args.iter().map(|s| FastStr::from(s.to_string())).collect()),
    };
    let resp = CLIENT.get_item(req).await.unwrap();
    match resp {
        GetItemResponse { ok, data } => {
            if ok {
                println!("{:?}", data.unwrap());
            } else {
                println!("Error: {:?}", data);
            }
        }
    }
}

#[volo::main]
async fn main() {
    tracing_subscriber::fmt::init();
    let stdin = io::stdin();
    for line in stdin.lock().lines() {
        let request = line.unwrap();
        handle(&request).await;
    }
}
