#![feature(impl_trait_in_assoc_type)]
#![feature(ascii_char)]
use std::{
    collections::HashMap,
    // fs::{File, OpenOptions},
    hash::Hash,
    // io::{BufRead, BufReader, Write},
    net::SocketAddr,
    sync::Mutex,
};

use pilota::FastStr;
use serde::Deserialize;
use volo_gen::volo::redis::{GetItemResponse, RedisCommand};
mod file_op;
use file_op::{remove_key_value_from_file, update_key_value_in_file, write_to_file};
// pub const DEFAULT_PROXY: &str = "127.0.0.1:8000";
pub const DEFAULT_ADDR: &str = "127.0.0.1:8080";
#[derive(PartialEq)]
pub enum Type {
    Master,
    Slave,
}

pub struct S {
    pub _type: Type,
    pub all_port: Mutex<Option<Vec<String>>>,
    pub map: Mutex<HashMap<String, String>>,
    pub _log_path: String,
}
#[derive(Debug, Deserialize, PartialEq, Hash)]
pub struct Range {
    pub start: u32,
    pub end: u32,
}
impl Eq for Range {}
#[derive(Debug)]
pub struct Proxy {
    pub proxy_addr: String,
    pub severs_addr: Mutex<HashMap<Range, String>>,
    pub addr_master: String,
}
#[volo::async_trait]
impl volo_gen::volo::redis::ItemService for Proxy {
    async fn get_item(
        &self,
        _req: volo_gen::volo::redis::GetItemRequest,
    ) -> ::core::result::Result<volo_gen::volo::redis::GetItemResponse, ::volo_thrift::AnyhowError>
    {
        let cmd = &_req.clone().cmd;
        let key: &str = &_req.clone().args.unwrap()[0].to_string();
        let mut addr = String::from(DEFAULT_ADDR);
        if *cmd == RedisCommand::Get {
            for item in self.severs_addr.lock().unwrap().iter() {
                if key.as_bytes()[0] >= item.0.start as u8 && key.as_bytes()[0] <= item.0.end as u8
                {
                    addr = item.1.clone();
                    break;
                }
            }
        }
        println!("SEND!");
        println!("addr: {}", addr);
        println!("_req: {:?}", _req);
        let addr: SocketAddr = addr.parse().unwrap();
        let addr = volo::net::Address::from(addr);
        let client = volo_gen::volo::redis::ItemServiceClientBuilder::new("my_redis")
            .layer_outer(LogLayer)
            .layer_outer(FilterLayer)
            .address(addr)
            .build();
        let resp = client.get_item(_req).await;
        Ok(match resp {
            Ok(GetItemResponse { ok, data }) => GetItemResponse { ok, data },
            Err(_) => GetItemResponse {
                ok: false,
                data: Some(FastStr::from("Error")),
            },
        })
    }
}

#[volo::async_trait]
impl volo_gen::volo::redis::ItemService for S {
    async fn get_item(
        &self,
        _req: volo_gen::volo::redis::GetItemRequest,
    ) -> ::core::result::Result<volo_gen::volo::redis::GetItemResponse, ::volo_thrift::AnyhowError>
    {
        match _req.cmd {
            RedisCommand::Get => {
                if let Some(arg) = _req.args {
                    if arg.len() != 1 {
                        Ok(GetItemResponse {
                            ok: false,
                            data: Some(FastStr::from(format!(
                                "Args Error! Expected 1, got {}",
                                arg.len()
                            ))),
                        })
                    } else {
                        if let Some(value) = self.map.lock().unwrap().get(&arg[0].to_string()) {
                            Ok(GetItemResponse {
                                ok: true,
                                data: Some(FastStr::from(value.to_string())),
                            })
                        } else {
                            Ok(GetItemResponse {
                                ok: false,
                                data: Some(FastStr::from("Error")),
                            })
                        }
                    }
                } else {
                    Ok(GetItemResponse {
                        ok: false,
                        data: Some(FastStr::from("Args Error!")),
                    })
                }
            }
            RedisCommand::Set => {
                if let Some(arg) = _req.args {
                    if arg.len() < 2 {
                        Ok(GetItemResponse {
                            ok: false,
                            data: Some(FastStr::from(format!(
                                "Args Error! Expected 2, got {}",
                                arg.len()
                            ))),
                        })
                    } else {
                        let (key, value) = (&arg[0], &arg[1]);
                        // 判断是不是主服务器
                        for addr in self.all_port.lock().unwrap().as_ref().unwrap() {
                            if addr == &DEFAULT_ADDR {
                                continue;
                            }
                            println!("addr: {}", addr);
                            let addr: SocketAddr = addr.parse().unwrap();
                            let addr = volo::net::Address::from(addr);
                            // 向从服务器发送请求
                            // TODO
                        }
                        if self
                            .map
                            .lock()
                            .unwrap()
                            .insert(key.to_string(), value.to_string())
                            .is_some()
                        {
                            update_key_value_in_file(&self._log_path, key, value).unwrap();
                            Ok(GetItemResponse {
                                ok: true,
                                data: Some(FastStr::from("Ok,Updated!")),
                            })
                        } else {
                            write_to_file(&self._log_path, key, value).unwrap();
                            Ok(GetItemResponse {
                                ok: true,
                                data: Some(FastStr::from("Ok, Insert Success!")),
                            })
                        }
                    }
                } else {
                    Ok(GetItemResponse {
                        ok: false,
                        data: Some(FastStr::from("Args Error!")),
                    })
                }
            }
            RedisCommand::Del => {
                if let Some(arg) = _req.args {
                    if arg.len() < 1 {
                        Ok(GetItemResponse {
                            ok: false,
                            data: Some(FastStr::from(format!(
                                "Args Error! Expected 1, got {}",
                                arg.len()
                            ))),
                        })
                    } else {
                        let mut count = 0;
                        for key in arg {
                            remove_key_value_from_file(&self._log_path, &key).unwrap();
                            count += self
                                .map
                                .try_lock()
                                .unwrap()
                                .remove(&(key.to_string()))
                                .is_some() as i32;
                        }
                        Ok(GetItemResponse {
                            ok: true,
                            data: Some(FastStr::from(format!("{}", count))),
                        })
                    }
                } else {
                    Ok(GetItemResponse {
                        ok: false,
                        data: Some(FastStr::from("Args Error!")),
                    })
                }
            }
            RedisCommand::Ping => {
                if let Some(arg) = _req.args {
                    if arg.len() == 0 {
                        return Ok(GetItemResponse {
                            ok: true,
                            data: Some(FastStr::from("PONG")),
                        });
                    } else {
                        Ok(GetItemResponse {
                            ok: true,
                            data: Some(FastStr::from(arg.join(" "))),
                        })
                    }
                } else {
                    Ok(GetItemResponse {
                        ok: true,
                        data: Some(FastStr::from("PONG")),
                    })
                }
            }
            RedisCommand::Publish => Ok(GetItemResponse {
                ok: false,
                data: Some(FastStr::from("Command Not impl!")),
            }),
            RedisCommand::Subscribe => Ok(GetItemResponse {
                ok: false,
                data: Some(FastStr::from("Command Not impl!")),
            }),
            _ => Ok(GetItemResponse {
                ok: false,
                data: Some(FastStr::from("Command Not Found!")),
            }),
        }
    }
}

pub struct LogLayer;

impl<S> volo::Layer<S> for LogLayer {
    type Service = LogService<S>;

    fn layer(self, inner: S) -> Self::Service {
        LogService(inner)
    }
}

#[derive(Clone)]
pub struct LogService<S>(S);

#[volo::service]
impl<Cx, Req, S> volo::Service<Cx, Req> for LogService<S>
where
    Req: std::fmt::Debug + Send + 'static,
    S: Send + 'static + volo::Service<Cx, Req> + Sync,
    S::Response: std::fmt::Debug,
    S::Error: std::fmt::Debug,
    Cx: Send + 'static,
{
    async fn call(&self, cx: &mut Cx, req: Req) -> Result<S::Response, S::Error> {
        let now = std::time::Instant::now();
        tracing::debug!("Received request {:?}", &req);
        let resp = self.0.call(cx, req).await;
        tracing::debug!("Sent response {:?}", &resp);
        tracing::info!("Request took {}ms", now.elapsed().as_millis());
        resp
    }
}
pub struct FilterLayer;

impl<S> volo::Layer<S> for FilterLayer {
    type Service = FilterService<S>;

    fn layer(self, inner: S) -> Self::Service {
        FilterService(inner)
    }
}
#[derive(Clone)]
pub struct FilterService<S>(S);
#[volo::service]
impl<Cx, Req, S> volo::Service<Cx, Req> for FilterService<S>
where
    Req: std::fmt::Debug + Send + 'static,
    S: Send + 'static + volo::Service<Cx, Req> + Sync,
    S::Response: std::fmt::Debug,
    S::Error: std::fmt::Debug,
    anyhow::Error: Into<S::Error>,
    Cx: Send + 'static,
{
    async fn call(&self, cx: &mut Cx, req: Req) -> Result<S::Response, S::Error> {
        let info = format!("{:?}", req);
        if info.contains("csj") {
            return Err(anyhow::anyhow!("[csj] is not allowed").into());
        }
        self.0.call(cx, req).await
    }
}
