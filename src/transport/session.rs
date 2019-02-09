//! `ClientSession` is an actor, it manages peer tcp connection and
//! proxies commands from peer to `ToyServer`.
use actix::prelude::*;
use std::io;
use std::time::{Duration, Instant};
use tokio_io::io::WriteHalf;
use tokio_tcp::TcpStream;

use super::codec::{ToyRequest, ToyResponse, ToyServerCodec};
use super::server::{self, ToyServer};

/// Toy server sends this kv pair to session
#[derive(Message)]
pub struct Next {
    pub key: String,
    pub value: String,
}

/// `ToySession` actor is responsible for tcp peer communications.
pub struct ToySession {
    /// unique session id
    id: usize,
    /// this is address of toy server
    addr: Addr<ToyServer>,
    /// Client must send ping at least once per 10 seconds, otherwise we drop
    /// connection.
    hb: Instant,
    /// Framed wrapper
    framed: actix::io::FramedWrite<WriteHalf<TcpStream>, ToyServerCodec>,
}

impl Actor for ToySession {
    type Context = actix::Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        // we'll start heartbeat process on session start.
        self.hb(ctx);

        // register self in toy server. `AsyncContext::wait` register
        // future within context, but context waits until this future resolves
        // before processing any other events.
        self.addr
            .send(server::Connect {
                addr: ctx.address(),
            })
            .into_actor(self)
            .then(|res, act, ctx| {
                match res {
                    Ok(res) => act.id = res,
                    // something is wrong with toy server
                    _ => ctx.stop(),
                }
                actix::fut::ok(())
            })
            .wait(ctx);
    }

    fn stopping(&mut self, _: &mut Self::Context) -> Running {
        // notify toy server
        self.addr.do_send(server::Disconnect { id: self.id });
        Running::Stop
    }
}

impl actix::io::WriteHandler<io::Error> for ToySession {}

/// To use `Framed` with an actor, we have to implement `StreamHandler` trait
impl StreamHandler<ToyRequest, io::Error> for ToySession {
    /// This is main event loop for client requests
    fn handle(&mut self, msg: ToyRequest, ctx: &mut Self::Context) {
        match msg {
            ToyRequest::Get(key) => {
                self.addr
                    .send(server::Get {
                        id: self.id,
                        key: key.clone(),
                    })
                    .into_actor(self) // <- create actor compatible future
                    .then(|res, act, _| {
                        match res {
                            Ok(value_result) => match value_result {
                                Ok(value) => act.framed.write(ToyResponse::Value(value)),
                                Err(e) => eprintln!("{}", e),
                            },
                            _ => eprintln!("Can not connect to toy server"),
                        }
                        actix::fut::ok(())
                    })
                    .wait(ctx)
            }
            ToyRequest::Put((k, v)) => {
                self.addr
                    .send(server::Put {
                        id: self.id,
                        key: k.clone(),
                        value: v.clone(),
                    })
                    .into_actor(self) // <- create actor compatible future
                    .then(move |res, act, _| {
                        match res {
                            Ok(put_result) => match put_result {
                                Ok(_) => {
                                    act.framed.write(ToyResponse::Saved((k.clone(), v.clone())))
                                }
                                Err(e) => eprintln!("{}", e),
                            },
                            _ => eprintln!("Can not connect to toy server"),
                        }
                        actix::fut::ok(())
                    })
                    .wait(ctx)
            }
            ToyRequest::Delete(k) => {
                self.addr
                    .send(server::Delete {
                        id: self.id,
                        key: k.clone(),
                    })
                    .into_actor(self) // <- create actor compatible future
                    .then(move |res, act, _| {
                        match res {
                            Ok(delete_res) => match delete_res {
                                Ok(_) => act.framed.write(ToyResponse::Deleted(k.clone())),

                                Err(e) => eprintln!("{}", e),
                            },
                            _ => eprintln!("Can not connect to toy server"),
                        }
                        actix::fut::ok(())
                    })
                    .wait(ctx)
            }

            // we update heartbeat time on ping from peer
            ToyRequest::Ping => self.hb = Instant::now(),
            ToyRequest::Scan => {
                self.addr.do_send(server::Scan(self.id));
            }
        }
    }
}

/// Helper methods
impl ToySession {
    pub fn new(
        addr: Addr<ToyServer>,
        framed: actix::io::FramedWrite<WriteHalf<TcpStream>, ToyServerCodec>,
    ) -> ToySession {
        ToySession {
            addr,
            framed,
            id: 0,
            hb: Instant::now(),
        }
    }

    /// helper method that sends ping to client every second.
    ///
    /// also this method check heartbeats from client
    fn hb(&self, ctx: &mut actix::Context<Self>) {
        ctx.run_later(Duration::new(1, 0), |act, ctx| {
            // check client heartbeats
            if Instant::now().duration_since(act.hb) > Duration::new(10, 0) {
                // heartbeat timed out
                println!("Client heartbeat failed, disconnecting!");

                // notify toy server
                act.addr.do_send(server::Disconnect { id: act.id });

                // stop actor
                ctx.stop();
            }

            act.framed.write(ToyResponse::Ping);
            act.hb(ctx);
        });
    }
}

impl Handler<Next> for ToySession {
    type Result = ();
    fn handle(&mut self, msg: Next, _: &mut Context<Self>) {
        let Next { key, value } = msg;
        self.framed.write(ToyResponse::Next((key, value)));
    }
}
