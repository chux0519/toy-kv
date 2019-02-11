use std::env;
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
    actix::System::run(|| {
        // Connect to server
        let args: Vec<String> = env::args().collect();
        if args.len() < 2 {
            eprintln!("Server address required!\nUsage: client [url:port]");
            std::process::exit(0);
        }
        let addr = &args[1];
        let addr = net::SocketAddr::from_str(addr).unwrap();
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
                    println!("Running toy client");
                    println!("Usage: $CMD [KEY?] [VALUE?], ie");
                    println!("\t Get [key]");
                    println!("\t Put [key] [value]");
                    println!("\t Delete [key]");
                    println!("\t Scan [start] [end]");
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
        let v: Vec<&str> = m.split(' ').collect();
        let cmd = v[0];

        if cmd == "Get" {
            if v.len() == 2 {
                self.framed.write(codec::ToyRequest::Get(v[1].to_owned()));
            } else {
                eprintln!("Wrong format, try `Get [key]`");
            }
        } else if cmd == "Put" {
            let v: Vec<&str> = m.split(' ').collect();
            if v.len() == 3 {
                self.framed
                    .write(codec::ToyRequest::Put((v[1].to_owned(), v[2].to_owned())));
            } else {
                eprintln!("Wrong format, try `Put [key] [value]`");
            }
        } else if cmd == "Delete" {
            let v: Vec<&str> = m.split(' ').collect();
            if v.len() == 2 {
                self.framed
                    .write(codec::ToyRequest::Delete(v[1].to_owned()));
            } else {
                eprintln!("Wrong format, try `Delete [key]`");
            }
        } else if cmd == "Scan" {
            if v.len() == 3 {
                let start = match v[1].parse::<u32>() {
                    Ok(n) => n,
                    _ => {
                        eprintln!("invalid input {}, set to 0", v[1]);
                        0
                    }
                };
                let end = match v[2].parse::<u32>() {
                    Ok(n) => n,
                    _ => {
                        eprintln!("invalid input {}, set to 0", v[2]);
                        0
                    }
                };
                self.framed.write(codec::ToyRequest::Scan((start, end)));
            } else {
                eprintln!("Wrong format, try `Scan start end`");
            }
        } else {
            eprintln!("Unknown command!")
        }
    }
}

/// Server communication
impl StreamHandler<codec::ToyResponse, io::Error> for ToyClient {
    fn handle(&mut self, msg: codec::ToyResponse, _: &mut Context<Self>) {
        match msg {
            codec::ToyResponse::Value(ref msg) => {
                if msg.is_empty() {
                    println!("not found");
                } else {
                    println!("{}", msg);
                }
            }
            codec::ToyResponse::Saved(ref msg) => {
                println!("({}, {}) saved", msg.0, msg.1);
            }
            codec::ToyResponse::Deleted(ref msg) => {
                println!("key({}) deleted", msg);
            }
            codec::ToyResponse::Next(ref msg) => {
                println!("({}, {})", msg.0, msg.1);
            }
            _ => (),
        }
    }
}
