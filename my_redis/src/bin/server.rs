#![feature(impl_trait_in_assoc_type)]
use std::{
    collections::HashMap,
    fs::File,
    // hash::Hash,
    io::{self, BufRead, Read},
    net::SocketAddr,
    path::Path,
    // string,
    sync::Mutex,
    // thread,
};

use my_redis::{
    FilterLayer, LogLayer, Proxy, Range,
    Type::{Master, Slave},
    S,
};
use toml::Value;
// use async_task::spawn;
const LOG_PATH: &str = "./DataBase/test.log";

#[volo::main]
async fn main() {
    tracing_subscriber::fmt::init();
    // 通过循环启动多个redis实例
    let config_path = "./Config/redis_1.conf";
    let proxy = read_conf(config_path);

    let _master_addr = proxy.addr_master.clone();
    let hash_map = read_log(LOG_PATH);
    // println!("{:?}", hash_map);
    let _addr: SocketAddr = proxy.proxy_addr.parse().unwrap();
    let mut tmp = Vec::new();
    let num = proxy.severs_addr.lock().unwrap().len();
    for value in proxy.severs_addr.lock().unwrap().values() {
        tmp.push(value.clone());
    }
    for i in 0..num {
        let _log_path = "./DataBase/test.log";
        let addr = tmp[i].clone();
        let mut _type = Master;
        if addr != proxy.addr_master {
            _type = Slave;
        }
        let s = S {
            _type,
            all_port: Mutex::new(Some(tmp.clone())),
            map: Mutex::new(hash_map.clone()),
            _log_path: _log_path.to_string(),
        };
        let addr: SocketAddr = addr.parse().unwrap();
        let addr = volo::net::Address::from(addr);
        tokio::spawn({
            volo_gen::volo::redis::ItemServiceServer::new(s)
                .layer_front(LogLayer)
                .layer_front(FilterLayer)
                .run(addr)
        });
    }

    let addr = proxy.proxy_addr.clone();
    println!("proxy_addr: {}", addr);
    let addr: SocketAddr = addr.parse().unwrap();
    let addr = volo::net::Address::from(addr);
    tokio::spawn({
        volo_gen::volo::redis::ItemServiceServer::new(proxy)
            .layer_front(LogLayer)
            .layer_front(FilterLayer)
            .run(addr)
    });

    tokio::signal::ctrl_c().await.unwrap();
}
fn read_log(file_path: &str) -> HashMap<String, String> {
    let mut hash_map = HashMap::new();
    if let Ok(lines) = read_lines(file_path) {
        // println!("xxxx");
        for line in lines {
            if let Ok(line) = line {
                let mut iter = line.split_whitespace();
                let key = iter.next().unwrap().to_string();
                let value = iter.next().unwrap().to_string();
                hash_map.insert(key, value);
            }
        }
    }
    // println!("{:?}", HashMap);
    hash_map
}
fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<File>>>
where
    P: AsRef<Path>,
{
    let file = File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}
fn read_conf(filepath: &str) -> Proxy {
    // 读取配置文件
    let mut file = File::open(filepath).unwrap();
    let mut contents = String::new();
    file.read_to_string(&mut contents).unwrap();

    // 解析配置文件
    let config: Value = toml::from_str(&contents).unwrap();

    // 获取配置信息并构建 Proxy 结构体
    let proxy_addr = config["proxy"]["proxy_addr"].as_str().unwrap().to_owned();
    let addr_master = config["proxy"]["addr_master"].as_str().unwrap().to_owned();

    let server_num = config["proxy"]["server_num"].as_integer().unwrap() as usize;
    let mut servers_addr = HashMap::new();

    for n in 1..=server_num {
        let server = &config[&format!("server{}", n)];

        let start = server["start"].as_integer().unwrap() as u32;
        let end = server["end"].as_integer().unwrap() as u32;
        let addr = server["addr"].as_str().unwrap().to_owned();

        servers_addr.insert(Range { start, end }, addr);
    }

    let proxy = Proxy {
        proxy_addr,
        severs_addr: Mutex::new(servers_addr),
        addr_master,
    };
    proxy
}
