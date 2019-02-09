#![allow(dead_code)]
use byteorder::{BigEndian, ByteOrder};
use bytes::{BufMut, BytesMut};
use serde::{Serialize, Deserialize};
use serde_json as json;
use std::io;
use tokio_io::codec::{Decoder, Encoder};
use actix::prelude::*;

/// Client request
#[derive(Serialize, Deserialize, Debug, Message)]
pub enum ToyRequest {
    /// Scan kv pairs
    Scan,
    /// Get the value of key
    Get(String),
    /// Send message
    Message(String),
    /// Ping
    Ping,
}

/// Server response
#[derive(Serialize, Deserialize, Debug, Message)]
pub enum ToyResponse {
    Ping,

    /// Scan of rooms
    Rooms(Vec<String>),

    /// Joined
    Joined(String),

    /// Message
    Message(String),
}

/// Codec for Client -> Server transport
pub struct ToyServerCodec;

impl Decoder for ToyServerCodec {
    type Item = ToyRequest;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let size = {
            if src.len() < 2 {
                return Ok(None);
            }
            BigEndian::read_u16(src.as_ref()) as usize
        };

        if src.len() >= size + 2 {
            src.split_to(2);
            let buf = src.split_to(size);
            Ok(Some(json::from_slice::<ToyRequest>(&buf)?))
        } else {
            Ok(None)
        }
    }
}

impl Encoder for ToyServerCodec {
    type Item = ToyResponse;
    type Error = io::Error;

    fn encode(
        &mut self, msg: ToyResponse, dst: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        let msg = json::to_string(&msg).unwrap();
        let msg_ref: &[u8] = msg.as_ref();

        dst.reserve(msg_ref.len() + 2);
        dst.put_u16_be(msg_ref.len() as u16);
        dst.put(msg_ref);

        Ok(())
    }
}

/// Codec for Server -> Client transport
pub struct ToyClientCodec;

impl Decoder for ToyClientCodec {
    type Item = ToyResponse;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let size = {
            if src.len() < 2 {
                return Ok(None);
            }
            BigEndian::read_u16(src.as_ref()) as usize
        };

        if src.len() >= size + 2 {
            src.split_to(2);
            let buf = src.split_to(size);
            Ok(Some(json::from_slice::<ToyResponse>(&buf)?))
        } else {
            Ok(None)
        }
    }
}

impl Encoder for ToyClientCodec {
    type Item = ToyRequest;
    type Error = io::Error;

    fn encode(
        &mut self, msg: ToyRequest, dst: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        let msg = json::to_string(&msg).unwrap();
        let msg_ref: &[u8] = msg.as_ref();

        dst.reserve(msg_ref.len() + 2);
        dst.put_u16_be(msg_ref.len() as u16);
        dst.put(msg_ref);

        Ok(())
    }
}