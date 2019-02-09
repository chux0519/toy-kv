use std::net;
use std::str::FromStr;

use actix::prelude::*;
use futures::Stream;
use tokio_codec::FramedRead;
use tokio_io::AsyncRead;
use tokio_tcp::{TcpListener, TcpStream};

use toy_kv::transport::codec::ToyServerCodec;
use toy_kv::transport::server::ToyServer;
use toy_kv::transport::session::ToySession;

/// Define tcp server that will accept incoming tcp connection and create
/// toy actors.
struct Server {
    toy: Addr<ToyServer>,
}

/// Make actor from `Server`
impl Actor for Server {
    /// Every actor has to provide execution `Context` in which it can run.
    type Context = Context<Self>;
}

#[derive(Message)]
struct TcpConnect(pub TcpStream, pub net::SocketAddr);

/// Handle stream of TcpStream's
impl Handler<TcpConnect> for Server {
    /// this is response for message, which is defined by `ResponseType` trait
    /// in this case we just return unit.
    type Result = ();

    fn handle(&mut self, msg: TcpConnect, _: &mut Context<Self>) {
        // For each incoming connection we create `ToySession` actor
        // with out toy server address.
        let server = self.toy.clone();
        ToySession::create(move |ctx| {
            let (r, w) = msg.0.split();
            ToySession::add_stream(FramedRead::new(r, ToyServerCodec), ctx);
            ToySession::new(server, actix::io::FramedWrite::new(w, ToyServerCodec, ctx))
        });
    }
}

fn main() {
    actix::System::run(|| {
        // Start toy server actor
        let server = ToyServer::default().start();

        // Create server listener
        let addr = net::SocketAddr::from_str("127.0.0.1:12345").unwrap();
        let listener = TcpListener::bind(&addr).unwrap();

        // Our toy server `Server` is an actor, first we need to start it
        // and then add stream on incoming tcp connections to it.
        // TcpListener::incoming() returns stream of the (TcpStream, net::SocketAddr)
        // items So to be able to handle this events `Server` actor has to implement
        // stream handler `StreamHandler<(TcpStream, net::SocketAddr), io::Error>`
        Server::create(|ctx| {
            ctx.add_message_stream(listener.incoming().map_err(|_| ()).map(|st| {
                let addr = st.peer_addr().unwrap();
                TcpConnect(st, addr)
            }));
            Server { toy: server }
        });

        println!("Running toy server on 127.0.0.1:12345");
    });
}