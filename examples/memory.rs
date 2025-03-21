use std::env;
use std::ffi::OsStr;

use fuse::Session;
use log::info;
use tokio::signal;
use tokio::sync::mpsc;

#[tokio::main]
async fn main() {
    env_logger::init();

    let mountpoint = env::args_os().nth(1).unwrap();
    // TODO: Understand the actual meaning of these parameters
    let options = ["-o", "rw", "-o", "fsname=memory"]
        .iter()
        .map(|o| o.as_ref())
        .collect::<Vec<&OsStr>>();

    let file_system_size = 4 << 30;
    let filesystem = fuse::memory::new(file_system_size);
    let mut session = Session::new(filesystem, mountpoint.as_ref(), &options).unwrap();

    let (tx, rx) = mpsc::channel(1);

    let ctrl_c = tokio::spawn(async move {
        signal::ctrl_c().await.expect("Failed to listen for Ctrl+C");
        info!("Received Ctrl+C, initiating shutdown...");
        tx.send(()).await.unwrap();
    });

    let background_task = tokio::spawn(async move {
        session.run_with_signal(rx).await.unwrap();
    });

    let (ctrl_c_result, _) = futures::join!(ctrl_c, background_task);
    if let Err(e) = ctrl_c_result {
        eprintln!("Error occurred in ctrl_c task: {:?}", e);
    }
}
