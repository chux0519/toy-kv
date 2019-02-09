//! `ToyServer` is an actor. It maintains list of connection client session.
//! And manages available rooms. Peers send messages to other peers in same
//! room through `ToyServer`.

use actix::prelude::*;
use rand::{self, Rng};
use std::collections::HashMap;

use super::super::engine::kv;
use super::super::engine::store::Store;
use super::open_db_from;
use super::session;
use std::path::PathBuf;

/// New toy session is created
pub struct Connect {
    pub addr: Addr<session::ToySession>,
}

/// Response type for Connect message
///
/// Toy server returns unique session id
impl actix::Message for Connect {
    type Result = usize;
}

/// Session is disconnected
#[derive(Message)]
pub struct Disconnect {
    pub id: usize,
}

/// Get value of key
pub struct Get {
    /// Client id
    pub id: usize,
    /// Room name
    pub key: String,
}

impl actix::Message for Get {
    type Result = String;
}

/// Put kv pair
#[derive(Message)]
pub struct Put {
    /// Client id
    pub id: usize,
    pub key: String,
    pub value: String,
}

/// Delete kek
#[derive(Message)]
pub struct Delete {
    /// Client id
    pub id: usize,
    pub key: String,
}

/// `ToyServer` manages toy rooms and responsible for coordinating toy
/// session. implementation is super primitive
pub struct ToyServer {
    sessions: HashMap<usize, Addr<session::ToySession>>,
    store: Store,
}

impl Default for ToyServer {
    fn default() -> ToyServer {
        let db_path: PathBuf = "toydb".parse().unwrap();
        ToyServer {
            sessions: HashMap::new(),
            store: open_db_from(&db_path).unwrap(),
        }
    }
}

/// Make actor from `ToyServer`
impl Actor for ToyServer {
    /// We are going to use simple Context, we just need ability to communicate
    /// with other actors.
    type Context = Context<Self>;
}

/// Handler for Connect message.
///
/// Register new session and assign unique id to this session
impl Handler<Connect> for ToyServer {
    type Result = usize;

    fn handle(&mut self, msg: Connect, _: &mut Context<Self>) -> Self::Result {
        // register session with random id
        let id = rand::thread_rng().gen::<usize>();
        self.sessions.insert(id, msg.addr);
        println!("client({}) connected", id);
        // send id back
        id
    }
}

/// Handler for Disconnect message.
impl Handler<Disconnect> for ToyServer {
    type Result = ();

    fn handle(&mut self, msg: Disconnect, _: &mut Context<Self>) {
        println!("client({}) disconnected", &msg.id);

        // remove address
        self.sessions.remove(&msg.id);
    }
}

/// Get value of key
impl Handler<Get> for ToyServer {
    type Result = String;

    fn handle(&mut self, msg: Get, _: &mut Context<Self>) -> Self::Result {
        let Get { id, key } = msg;
        let value = self.store.get(key.parse().unwrap()).unwrap();
        match value {
            None => "".to_owned(),
            Some(v) => v.to_string(),
        }
    }
}

/// Put kv pair
impl Handler<Put> for ToyServer {
    type Result = ();

    fn handle(&mut self, msg: Put, _: &mut Context<Self>) {
        let Put { id, key, value } = msg;
        self.store
            .put(
                key.parse().unwrap(),
                kv::Value::Valid(Box::new(value.parse().unwrap())),
            )
            .unwrap();
    }
}

/// Delete value of key
impl Handler<Delete> for ToyServer {
    type Result = ();

    fn handle(&mut self, msg: Delete, _: &mut Context<Self>) {
        let Delete { id, key } = msg;
        self.store.delete(key.parse().unwrap()).unwrap();
    }
}
