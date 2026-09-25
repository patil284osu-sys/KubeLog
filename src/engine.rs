use crate::storage::{Page, Store};
use std::{
    sync::{
        Arc, Mutex,
        mpsc::{self, Receiver, SyncSender, TrySendError},
    },
    thread::{self, JoinHandle},
};

const QUEUE_ITEMS: usize = 1024;
const QUEUE_BYTES: usize = 16 * 1024 * 1024;

struct State {
    accepting: bool,
    healthy: bool,
    count: usize,
    bytes: usize,
}

struct Request {
    payload: Vec<u8>,
    reply: mpsc::Sender<Result<u64, String>>,
}

enum Message {
    Append(Request),
    Stop,
}

pub struct Description {
    pub end: u64,
    pub segments: usize,
    pub queued: usize,
    pub bytes: usize,
    pub healthy: bool,
}

pub struct Engine {
    sender: SyncSender<Message>,
    state: Arc<Mutex<State>>,
    store: Arc<Mutex<Store>>,
    writer: Mutex<Option<JoinHandle<()>>>,
}

fn writer_loop(store: Arc<Mutex<Store>>, state: Arc<Mutex<State>>, requests: Receiver<Message>) {
    while let Ok(message) = requests.recv() {
        let Message::Append(request) = message else {
            break;
        };
        let size = request.payload.len();
        let result = store
            .lock()
            .unwrap()
            .append(&request.payload)
            .map_err(|err| err.to_string());
        {
            let mut status = state.lock().unwrap();
            status.bytes -= size;
            status.count -= 1;
            if let Err(message) = &result {
                if !message.contains("capacity reached")
                    && !message.contains("record exceeds segment limit")
                {
                    status.healthy = false;
                    status.accepting = false;
                }
            }
        }
        let _ = request.reply.send(result);
    }
}

impl Engine {
    pub fn start(store: Store) -> Self {
        let store = Arc::new(Mutex::new(store));
        let state = Arc::new(Mutex::new(State {
            accepting: true,
            healthy: true,
            count: 0,
            bytes: 0,
        }));
        let (sender, receiver) = mpsc::sync_channel(QUEUE_ITEMS);
        let writer_store = Arc::clone(&store);
        let writer_state = Arc::clone(&state);
        let writer = thread::spawn(move || writer_loop(writer_store, writer_state, receiver));
        Self {
            sender,
            store,
            state,
            writer: Mutex::new(Some(writer)),
        }
    }

    pub fn append(&self, payload: Vec<u8>) -> Result<u64, String> {
        if payload.len() > crate::format::MAX_PAYLOAD {
            return Err("payload too large".into());
        }
        let (reply, completion) = mpsc::channel();
        {
            let mut status = self.state.lock().unwrap();
            if !status.healthy || !status.accepting {
                return Err("engine unavailable".into());
            }
            if status.count == QUEUE_ITEMS || status.bytes + payload.len() > QUEUE_BYTES {
                return Err("engine overloaded".into());
            }
            status.count += 1;
            status.bytes += payload.len();
            let size = payload.len();
            if let Err(err) = self
                .sender
                .try_send(Message::Append(Request { payload, reply }))
            {
                status.count -= 1;
                status.bytes -= size;
                return Err(match err {
                    TrySendError::Full(_) => "engine overloaded",
                    TrySendError::Disconnected(_) => "engine unavailable",
                }
                .into());
            }
        }
        completion
            .recv()
            .unwrap_or_else(|_| Err("append outcome unknown; writer stopped".into()))
    }

    pub fn read(&self, offset: u64, records: u32, bytes: u32) -> Result<Page, String> {
        if !self.state.lock().unwrap().healthy {
            return Err("engine unavailable".into());
        }
        let result = self.store.lock().unwrap().read_page(offset, records, bytes);
        if let Err(err) = &result {
            let message = err.to_string();
            if !message.starts_with("buffer too small:")
                && message != "offset out of range"
                && message != "invalid read limits"
            {
                let mut state = self.state.lock().unwrap();
                state.healthy = false;
                state.accepting = false;
            }
        }
        result.map_err(|err| err.to_string())
    }

    pub fn describe(&self) -> Description {
        let store = self.store.lock().unwrap();
        let state = self.state.lock().unwrap();
        Description {
            end: store.end(),
            segments: store.segment_count(),
            queued: state.count,
            bytes: state.bytes,
            healthy: state.healthy && state.accepting,
        }
    }

    pub fn stop_admission(&self) {
        self.state.lock().unwrap().accepting = false;
    }

    pub fn shutdown(&self) {
        self.state.lock().unwrap().accepting = false;
        let mut handle = self.writer.lock().unwrap();
        if let Some(handle) = handle.take() {
            let _ = self.sender.send(Message::Stop);
            let _ = handle.join();
        }
    }
}
