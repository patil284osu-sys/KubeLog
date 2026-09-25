use kubelog::{engine::Engine, server::serve, storage::Store};
use std::{
    env,
    error::Error,
    net::TcpListener,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

fn run() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();
    match args.as_slice() {
        [_, command, path] if command == "init" => {
            Store::init(Path::new(path))?;
            println!("initialized {path}");
        }
        [_, command, path] | [_, command, path, _] if command == "serve" => {
            let address = args.get(3).map(String::as_str).unwrap_or("127.0.0.1:7676");
            let store = Store::open(Path::new(path))?;
            let engine = Arc::new(Engine::start(store));
            let listener = TcpListener::bind(address)?;
            let stop = Arc::new(AtomicBool::new(false));
            let signal = Arc::clone(&stop);
            ctrlc::set_handler(move || signal.store(true, Ordering::SeqCst))?;
            println!("listening on {}", listener.local_addr()?);
            let result = serve(listener, Arc::clone(&engine), stop);
            engine.shutdown();
            result?;
        }
        _ => {
            return Err(
                "usage: kubelog init DATA_DIR | kubelog serve DATA_DIR [127.0.0.1:PORT]".into(),
            );
        }
    }
    Ok(())
}

fn main() {
    if let Err(err) = run() {
        eprintln!("kubelog: {err}");
        std::process::exit(1);
    }
}
