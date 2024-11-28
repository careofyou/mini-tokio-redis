use bytes::BytesMut;
use tokio::net::TcpStream;
use tokio::io::{self, BufWriter, AsyncReadExt};
use bytes::Buf;
use mini_redis::{Frame, Result};
use mini_redis::frame::Error::Incomplete;
use std::io::Cursor;

pub struct Connection {
    stream: BufWriter<TcpStream>,
    buffer: BytesMut,
}

impl Connection {
    pub fn new(stream: TcpStream) -> Connection {
        Connection {
            stream: BufWriter::new(stream),
            // allocate the buffer with 4kb of copacity.
            buffer: BytesMut::with_capacity(4096),
        }
    }
}

pub async fn read_frame(&mut self)
    -> Result<Option<Frame>>
{
    loop {
        // attempt to parse a frame from the buffered data
        // if enough data has been buffered, the frame is returned
        if let Some(frame) = self.parse_frame()? {
            return Ok(Some(frame));
        } 
        // there is not enough buffered data to read a frame
        // attempt to read more data from the socket 
        // on success the number of bytes is returned
        // '0' indicates "end of stream".
        if 0 == self.stram.read_buff(&mut self.buffer).await? {
            // the remote close the connection, for a clean shutdown
            // there is shouldnt be any data in the read buffer
            // if there is, meaning thaat the peer closed the scoket
            // while sending a frame.
            if self.buffer.is_empty() {
                return Ok(None);
            } else {
                return Err("connection reset by peer.".into());
            }
        }
    }
}

fn parse_frame(&mut self)
    -> Result<Option<Frame>>
{
    // Create the 'T:Buf' type
    let mut buf = Cursor::new(&self.buffer[..]);

    // Check weather a full frame is avaliable 
    match Frame::check(&mut buf) {
        Ok(_) => {
            // get the byte length of the frame
            let len = buf.position() as usize;

            // reset the internal cursor for the call to 'parse'
            buf.set_position(0);

            // parse the frame
            let frame = Frame::parse(&mut buf)?;

            // discard the frame from the buffer
            self.buffer.advance(len);

            // return the frame to the caller
            Ok(Some(frame))
        }
        // not enough data has been buffered
        Err(Incomplete) => Ok(None),
        // an error was encounterd
        Err(e) => Err(e.into()),
    }
}

async fn write_fram(&mut self, frame: &Frame)
    -> io::Result<()>
{
    match frame {
        Frame::Simple(val) => {
            self.stream.write_u8(b'+').await?;
            self.stream.write_all(val.as_bytes()).await?;
            self.stream.write_all(b"\r\n").await?;
        }
        Frame::Error(val) => {
            self.stream.write_u8(b'-').await?;
            self.stream.write_all(val.as_bytes()).await?;
            self.stream.write_all(b"\r\n").await?;
        }
        Frame::Integer(val) => {
            self.stream.write_u8(b':').await?;
            self.write_decimal(*val).await?;
        }
        Frame::Null => {
            self.stream.write_all(b"$-1\r\n").await?;
        }
        Frame::Bulk(val) => {
            let len = val.len();

            self.stream.write_u8(b'$').await?;
            self.write_decimal(len as u64).await?;
            self.stream.write_all(val).await?;
            self.stream.write_all(b"\r\n").await?;
        }
        Frame::Array(_val) => unimplemented!(),
    }

    self.stream.flush().await?;

    Ok(())
}