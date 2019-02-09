use std::str::FromStr;
use std::time::Duration;
use std::{io, net, process, thread};

use actix::prelude::*;
use futures::Future;
use tokio_codec::FramedRead;
use tokio_io::io::WriteHalf;
use tokio_io::AsyncRead;
use tokio_tcp::TcpStream;

use toy_kv::transport::codec;

fn main() {
    println!("Running toy client");

    actix::System::run(|| {
        // Connect to server
        let addr = net::SocketAddr::from_str("127.0.0.1:12345").unwrap();
        Arbiter::spawn(
            TcpStream::connect(&addr)
                .and_then(|stream| {
                    let addr = ToyClient::create(|ctx| {
                        let (r, w) = stream.split();
                        ctx.add_stream(FramedRead::new(r, codec::ToyClientCodec));
                        ToyClient {
                            framed: actix::io::FramedWrite::new(w, codec::ToyClientCodec, ctx),
                        }
                    });

                    // start console loop
                    thread::spawn(move || loop {
                        let mut cmd = String::new();
                        if io::stdin().read_line(&mut cmd).is_err() {
                            println!("error");
                            return;
                        }

                        addr.do_send(ClientCommand(cmd));
                    });

                    futures::future::ok(())
                })
                .map_err(|e| {
                    println!("Can not connect to server: {}", e);
                    process::exit(1)
                }),
        );
    });
}

struct ToyClient {
    framed: actix::io::FramedWrite<WriteHalf<TcpStream>, codec::ToyClientCodec>,
}

#[derive(Message)]
struct ClientCommand(String);

impl Actor for ToyClient {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Context<Self>) {
        // start heartbeats otherwise server will disconnect after 10 seconds
        self.hb(ctx)
    }

    fn stopping(&mut self, _: &mut Context<Self>) -> Running {
        println!("Disconnected");

        // Stop application on disconnect
        System::current().stop();

        Running::Stop
    }
}

impl ToyClient {
    fn hb(&self, ctx: &mut Context<Self>) {
        ctx.run_later(Duration::new(1, 0), |act, ctx| {
            act.framed.write(codec::ToyRequest::Ping);
            act.hb(ctx);
        });
    }
}

impl actix::io::WriteHandler<io::Error> for ToyClient {}

/// Handle stdin commands
impl Handler<ClientCommand> for ToyClient {
    type Result = ();

    fn handle(&mut self, msg: ClientCommand, _: &mut Context<Self>) {
        let m = msg.0.trim();

        // we check for /sss type of messages
        if m.starts_with('/') {
            let v: Vec<&str> = m.split(' ').collect();
            match v[0] {
                "/scan" => {
                    self.framed.write(codec::ToyRequest::Scan);
                }
                "/get" => {
                    if v.len() == 2 {
                        self.framed.write(codec::ToyRequest::Get(v[1].to_owned()));
                    } else {
                        println!("!!! key is required");
                    }
                }
                "/put" => {
                    if v.len() == 3 {
                        self.framed
                            .write(codec::ToyRequest::Put((v[1].to_owned(), v[2].to_owned())));
                    } else {
                        println!("!!! key and value is required");
                    }
                }
                "/delete" => {
                    if v.len() == 2 {
                        self.framed
                            .write(codec::ToyRequest::Delete(v[1].to_owned()));
                    } else {
                        println!("!!! key is required");
                    }
                }
                _ => println!("!!! unknown command"),
            }
        } else {
            println!("try `/get key`, `/put key value`, `/delete key` and `/scan`")
        }
    }
}

/// Server communication
impl StreamHandler<codec::ToyResponse, io::Error> for ToyClient {
    fn handle(&mut self, msg: codec::ToyResponse, _: &mut Context<Self>) {
        match msg {
            codec::ToyResponse::Value(ref msg) => {
                println!("value: {}", msg);
            }
            codec::ToyResponse::Saved(ref msg) => {
                println!("saved: {}", msg);
            }
            codec::ToyResponse::Deleted(ref msg) => {
                println!("deleted: {}", msg);
            }
            codec::ToyResponse::Next(ref msg) => {
                println!("({}, {})", msg.0, msg.1);
            }
            _ => (),
        }
    }
}
