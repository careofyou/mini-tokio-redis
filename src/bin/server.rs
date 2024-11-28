use tokio::net::{TcpListener, TcpStream};
use mini_redis::{Connection, Frame};
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

type Db = Arc<Mutex<HashMap<String, Bytes>>>;


#[tokio::main]
async fn main() {
    //bind the listener to the address 
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    println!("Listening");

    let db = Arc::new(Mutex::new(HashMap::new()));
    loop {
        // the second ite, contains the IP and port of the new connection.
        let (socket, _) = listener.accept().await.unwrap();
        // Clone the handle to the hash map
        let db = db.clone();
        // a new task is spawned for each inbound socket. the socket is
        // moved to the new task and processed
        tokio::spawn(async move {
            process(socket, db).await;
        });
    }
}

async fn process(socket: TcpStream, db:Db) {
    use mini_redis::Command::{self, Get, Set};

    // Connection probided by 'mini-redis', handled
    // parsing frames from the socket
    // connection provided by 'mini-redis', handles parsing frames from the socket
    // the 'connection' lets to read/write redis **frames** insted of 
    // byte streams. The 'Connection' type is defined by mini-redis
    let mut connection = Connection::new(socket);

    // use 'read_frame' to recive a command from the connection
    while let Some(frame) = connection.read_frame().await.unwrap() {
        let response = match Command::from_frame(frame).unwrap() {
            Set(cmd) => {
                let mut db = db.lock().unwrap();
                // the value is stored as 'Vec<u8>'
                db.insert(cmd.key().to_string(), cmd.value().clone());
                Frame::Simple("OK".to_string())
            }
            Get(cmd) => {
                let db = db.lock().unwrap();
                if let Some(value) = db.get(cmd.key()) {
                    // 'Frame::Bulk" expects data to be of type 'Bytes'. 
                    // This type will be covered later.
                    Frame::Bulk(value.clone())
                } else {
                    Frame::Null
                }
            }
                cmd => panic!("unimplemented: {:?}", cmd),
        };
        connection.write_frame(&response).await.unwrap();
    }
}