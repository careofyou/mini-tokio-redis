use bytes::Bytes;
use mini_redis::client;
use tokio::sync::{mpsc, oneshot};

#[derive(Debug)]
enum Command {
    Get {
        key: String,
        resp: Responder<Option<Bytes>>,
    },
    Set {
        key: String,
        val: Bytes,
        resp: Responder<()>,
    },
}

// Provided by the requester and used by the manager task to send the command 
// response back to the requester.
type Responder<T> = oneshot::Sender<mini_redis::Result<T>>;

#[tokio::main]
async fn main() {
    // create a new channel with a capacity of at most 32
    let (tx, mut rx) = mpsc::channel(32);

    // clone a 'tx' handle to the second f 
    let tx2 = tx.clone();

    // the 'move' keyword is used to **move** ownership of 'rx' into the task
    let manager = tokio::spawn(async move {
        // establish a connection to the server 
        let mut client = client::connect("127.0.0.1:6379").await.unwrap();

        // start receiving messages 
        while let Some(cmd) = rx.recv().await {
            match cmd {
                Command::Get { key, resp } => {
                    let res = client.get(&key).await;
                    // ignoring the errors
                    let _ = resp.send(res);
                }
                Command::Set {key, val, resp } => {
                    let res = client.set(&key, val).await;
                    // ignoring the errors 
                    let _ = resp.send(res);
                }
            }
        }
    });

    // spawn two tasks, one get a key, the other sets a key
    let t1 = tokio::spawn(async move{
        let (resp_tx, resp_rx) = oneshot::channel();
        let cmd = Command::Get {
            key: "foo".to_string(),
            resp: resp_tx,
        };

        tx.send(cmd).await.unwrap();
    });

    let t2 = tokio::spawn(async move{
        let (resp_tx, resp_rx) = oneshot::channel();
        let cmd = Command::Set {
            key: "foo".to_string(),
            val: "bar".into(),
            resp: resp_tx,
        };

        // send the SET request
        tx2.send(cmd).await.unwrap();

        let res = resp_rx.await;
        println!("GOT = {:?}", res);
    });

    t1.await.unwrap();
    t2.await.unwrap();
    manager.await.unwrap();
}