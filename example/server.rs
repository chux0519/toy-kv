use std::env;
use std::net;
use std::path::PathBuf;

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

/// Environment
static DB_DIR: &str = "DB_DIR";
static SERVER_PORT: &str = "SERVER_PORT";

fn main() {
    let db_dir: PathBuf = env::var(DB_DIR)
        .unwrap_or_else(|_| "toydb".to_owned())
        .parse()
        .unwrap();
    let port = env::var(SERVER_PORT).unwrap_or_else(|_| "8888".to_owned());

    actix::System::run(move || {
        // Start toy server actor
        let server = ToyServer::new(db_dir).start();

        // Create server listener
        use std::net::{IpAddr, Ipv4Addr, SocketAddr};
        let addr = SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
            port.parse::<u16>().unwrap(),
        );
        // let addr = net::SocketAddr::from_str("0.0.0.0:12345").unwrap();
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

        println!("Running toy server on {}", &addr);
    });
}
