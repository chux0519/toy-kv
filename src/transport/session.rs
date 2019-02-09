//! `ClientSession` is an actor, it manages peer tcp connection and
//! proxies commands from peer to `ToyServer`.
use actix::prelude::*;
use std::io;
use std::time::{Duration, Instant};
use tokio_io::io::WriteHalf;
use tokio_tcp::TcpStream;

use super::codec::{ToyServerCodec, ToyRequest, ToyResponse};
use super::server::{self, ToyServer};

/// Toy server sends this messages to session
#[derive(Message)]
pub struct Message(pub String);

/// `ToySession` actor is responsible for tcp peer communications.
pub struct ToySession {
    /// unique session id
    id: usize,
    /// this is address of toy server
    addr: Addr<ToyServer>,
    /// Client must send ping at least once per 10 seconds, otherwise we drop
    /// connection.
    hb: Instant,
    /// joined room
    room: String,
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
            ToyRequest::Scan => {
                // Send ListRooms message to toy server and wait for response
                println!("Scan rooms");
                self.addr.send(server::ListRooms)
                    .into_actor(self)     // <- create actor compatible future
                    .then(|res, act, _| {
                        match res {
                            Ok(rooms) => act.framed.write(ToyResponse::Rooms(rooms)),
                            _ => println!("Something is wrong"),
                        }
                        actix::fut::ok(())
                    }).wait(ctx)
                // .wait(ctx) pauses all events in context,
                // so actor wont receive any new messages until it get list of rooms back
            }
            ToyRequest::Get(name) => {
                println!("Get to room: {}", name);
                self.room = name.clone();
                self.addr.do_send(server::Get {
                    id: self.id,
                    name: name.clone(),
                });
                self.framed.write(ToyResponse::Joined(name));
            }
            ToyRequest::Message(message) => {
                // send message to toy server
                println!("Peer message: {}", message);
                self.addr.do_send(server::Message {
                    id: self.id,
                    msg: message,
                    room: self.room.clone(),
                })
            }
            // we update heartbeat time on ping from peer
            ToyRequest::Ping => self.hb = Instant::now(),
        }
    }
}

/// Handler for Message, toy server sends this message, we just send string to
/// peer
impl Handler<Message> for ToySession {
    type Result = ();

    fn handle(&mut self, msg: Message, _: &mut Self::Context) {
        // send message to peer
        self.framed.write(ToyResponse::Message(msg.0));
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
            room: "Main".to_owned(),
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