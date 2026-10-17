//! System mode from the app's side: the elevated helper (`sonicnet-system.exe`) is launched with
//! one UAC prompt, talks to us over a pipe we create, and is told to stop when System mode is
//! turned off or the app quits. The app itself stays unelevated.
//!
//! Should the app die without saying `stop`, the helper notices on its own (it watches our
//! process) and puts routing back; see `sonic_system::helper`.

use std::io;
use std::path::Path;
use std::time::Duration;

use sonic_system::protocol::{self, AppMessage, HelperMessage, NetStats};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::{mpsc, oneshot};

use crate::shell::Shell;

/// How long quitting (or turning System mode off) waits for the helper to put routing back.
pub const STOP_WAIT: Duration = Duration::from_secs(3);

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum State {
    #[default]
    Off,
    Starting,
    On,
    /// The user said no to the UAC prompt.
    NeedsPermission,
    Failed(String),
}

/// What the window shows: the state and, while on, each network's last second.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Status {
    pub state: State,
    pub networks: Vec<NetStats>,
}

#[derive(Debug)]
pub enum Request {
    /// Start the helper unless it runs (or is starting) already.
    Enable,
    Disable,
    /// Stop the helper, wait for it, then answer; the supervisor ends.
    Shutdown(oneshot::Sender<()>),
}

pub type Requests = mpsc::UnboundedSender<Request>;

/// Starts the supervisor on `runtime`. Statuses go to the window through `to_ui`.
pub fn spawn(runtime: &tokio::runtime::Handle, to_ui: mpsc::UnboundedSender<Shell>) -> Requests {
    let (tx, rx) = mpsc::unbounded_channel();
    runtime.spawn(supervise(rx, to_ui));
    tx
}

/// Stops the helper (if any) and waits for it, at most [`STOP_WAIT`] plus a little.
pub async fn shutdown(requests: &Requests) {
    let (done, finished) = oneshot::channel();
    if requests.send(Request::Shutdown(done)).is_ok() {
        let _ = tokio::time::timeout(STOP_WAIT + Duration::from_secs(1), finished).await;
    }
}

fn publish(to_ui: &mpsc::UnboundedSender<Shell>, state: State, networks: Vec<NetStats>) {
    let _ = to_ui.send(Shell::System(Status { state, networks }));
}

async fn supervise(
    mut requests: mpsc::UnboundedReceiver<Request>,
    to_ui: mpsc::UnboundedSender<Shell>,
) {
    let mut session: Option<(oneshot::Sender<()>, tokio::task::JoinHandle<()>)> = None;
    while let Some(request) = requests.recv().await {
        match request {
            Request::Enable => {
                if session.as_ref().is_some_and(|(_, task)| !task.is_finished()) {
                    continue;
                }
                let (stop, stopped) = oneshot::channel();
                let to_ui = to_ui.clone();
                let task = tokio::spawn(async move {
                    publish(&to_ui, State::Starting, Vec::new());
                    // `None`: stopped on request; the supervisor says what comes next.
                    if let Some(state) = run_session(&to_ui, stopped).await {
                        publish(&to_ui, state, Vec::new());
                    }
                });
                session = Some((stop, task));
            }
            Request::Disable => {
                stop(&mut session).await;
                publish(&to_ui, State::Off, Vec::new());
            }
            Request::Shutdown(done) => {
                stop(&mut session).await;
                let _ = done.send(());
                return;
            }
        }
    }
    stop(&mut session).await;
}

async fn stop(session: &mut Option<(oneshot::Sender<()>, tokio::task::JoinHandle<()>)>) {
    if let Some((stop, task)) = session.take() {
        let _ = stop.send(());
        let _ = tokio::time::timeout(STOP_WAIT + Duration::from_millis(500), task).await;
    }
}

/// What a failed launch means for the user.
fn launch_failure(error: &io::Error) -> State {
    const ERROR_CANCELLED: i32 = 1223;
    if error.raw_os_error() == Some(ERROR_CANCELLED) {
        State::NeedsPermission
    } else {
        State::Failed(format!("Couldn't start System mode: {error}"))
    }
}

#[cfg(not(windows))]
async fn run_session(_: &mpsc::UnboundedSender<Shell>, _: oneshot::Receiver<()>) -> Option<State> {
    Some(State::Failed("System mode needs Windows".into()))
}

/// One run of the helper, from launch to exit. `None` when it ended because we asked.
#[cfg(windows)]
async fn run_session(
    to_ui: &mpsc::UnboundedSender<Shell>,
    mut stop: oneshot::Receiver<()>,
) -> Option<State> {
    let exe_dir = match std::env::current_exe() {
        Ok(exe) => exe.parent().map(Path::to_path_buf).unwrap_or_default(),
        Err(error) => return Some(State::Failed(format!("Can't find SonicNet's folder: {error}"))),
    };
    let helper = exe_dir.join("sonicnet-system.exe");
    if !helper.is_file() {
        return Some(State::Failed("sonicnet-system.exe is missing from SonicNet's folder".into()));
    }
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.subsec_nanos());
    let pipe_name = format!(r"\\.\pipe\SonicNet.System.{}.{nonce:08x}", std::process::id());
    let server = match pipe::create(&pipe_name) {
        Ok(server) => server,
        Err(error) => {
            return Some(State::Failed(format!("Couldn't open a pipe for System mode: {error}")));
        }
    };
    let params = format!(
        "--parent-pid {} --pipe \"{pipe_name}\" --wintun \"{}\"",
        std::process::id(),
        exe_dir.join("wintun.dll").display()
    );
    // The UAC prompt blocks the launching thread until the user answers.
    let launch = tokio::task::spawn_blocking(move || launch::elevated(&helper, &params, "runas"));
    let process = tokio::select! {
        launched = launch => match launched {
            Ok(Ok(process)) => process,
            Ok(Err(error)) => return Some(launch_failure(&error)),
            Err(error) => return Some(State::Failed(format!("Couldn't start System mode: {error}"))),
        },
        // Turned off or quitting while the prompt is up. Should the user still say yes, the
        // helper finds no pipe and exits without touching routing.
        _ = &mut stop => return None,
    };
    let connected = tokio::select! {
        connected = tokio::time::timeout(Duration::from_secs(15), server.connect()) => connected,
        _ = &mut stop => return None,
    };
    match connected {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            return Some(State::Failed(format!("System mode didn't connect: {error}")));
        }
        Err(_) => return Some(State::Failed("System mode didn't answer".into())),
    }
    // Whoever connected must be the process we launched, not something squatting on the name.
    if !pipe::client_is(&server, &process) {
        return Some(State::Failed("Something else answered on System mode's pipe".into()));
    }

    let (reader, mut writer) = tokio::io::split(server);
    let mut lines = BufReader::new(reader).lines();
    let mut reason = None;
    let mut asked_to_stop = false;
    let mut state = State::Starting;
    // Once asked to stop, the helper gets STOP_WAIT to hang up before we stop listening.
    let give_up = tokio::time::sleep(Duration::from_secs(365 * 24 * 3600));
    tokio::pin!(give_up);
    loop {
        tokio::select! {
            line = lines.next_line() => match line {
                Ok(Some(line)) => match protocol::parse::<HelperMessage>(&line) {
                    Some(HelperMessage::Starting) => {}
                    Some(HelperMessage::On) => {
                        state = State::On;
                        publish(to_ui, State::On, Vec::new());
                    }
                    Some(HelperMessage::Stats { networks }) if state == State::On => {
                        tracing::debug!(?networks, "System mode stats");
                        publish(to_ui, State::On, networks);
                    }
                    Some(HelperMessage::Stats { .. }) => {}
                    Some(HelperMessage::Failed { reason: why }) => reason = Some(why),
                    None => tracing::debug!("System mode said something unknown: {line}"),
                },
                // The helper hung up: it has stopped (or is about to).
                Ok(None) | Err(_) => break,
            },
            _ = &mut stop, if !asked_to_stop => {
                asked_to_stop = true;
                give_up.as_mut().reset(tokio::time::Instant::now() + STOP_WAIT);
                let said = writer.write_all(protocol::line(&AppMessage::Stop).as_bytes()).await;
                if said.is_err() {
                    break;
                }
            }
            () = &mut give_up => break,
        }
    }
    let started = std::time::Instant::now();
    let exited =
        tokio::task::spawn_blocking(move || process.wait(STOP_WAIT)).await.unwrap_or(false);
    tracing::info!(exited, ms = started.elapsed().as_millis() as u64, "System mode helper ended");
    if asked_to_stop {
        None
    } else {
        Some(State::Failed(reason.unwrap_or_else(|| "System mode stopped unexpectedly".into())))
    }
}

#[cfg(windows)]
mod pipe {
    use std::os::windows::io::AsRawHandle;

    use tokio::net::windows::named_pipe::{NamedPipeServer, ServerOptions};
    use windows_sys::Win32::System::Pipes::GetNamedPipeClientProcessId;
    use windows_sys::Win32::System::Threading::GetProcessId;

    use super::launch::Process;

    /// Only this process may serve the name, and only local clients are admitted. The default
    /// security gives our own user (which the elevated helper runs as) full access.
    pub fn create(name: &str) -> std::io::Result<NamedPipeServer> {
        ServerOptions::new().first_pipe_instance(true).reject_remote_clients(true).create(name)
    }

    pub fn client_is(server: &NamedPipeServer, process: &Process) -> bool {
        let mut client = 0u32;
        // SAFETY: the pipe handle is open for the borrow; `client` is a valid out pointer.
        let known = unsafe { GetNamedPipeClientProcessId(server.as_raw_handle(), &mut client) };
        // SAFETY: the process handle is open while `process` lives.
        let launched = unsafe { GetProcessId(process.raw()) };
        known != 0 && launched != 0 && client == launched
    }
}

#[cfg(windows)]
mod launch {
    use std::io;
    use std::path::Path;
    use std::time::Duration;

    use windows_sys::Win32::Foundation::{CloseHandle, HANDLE, WAIT_OBJECT_0};
    use windows_sys::Win32::System::Com::{
        COINIT_APARTMENTTHREADED, COINIT_DISABLE_OLE1DDE, CoInitializeEx, CoUninitialize,
    };
    use windows_sys::Win32::System::Threading::WaitForSingleObject;
    use windows_sys::Win32::UI::Shell::{
        SEE_MASK_FLAG_NO_UI, SEE_MASK_NOASYNC, SEE_MASK_NOCLOSEPROCESS, SHELLEXECUTEINFOW,
        ShellExecuteExW,
    };
    use windows_sys::Win32::UI::WindowsAndMessaging::SW_HIDE;

    /// A process we launched, closed on drop.
    #[derive(Debug)]
    pub struct Process(HANDLE);

    // SAFETY: a process handle is a kernel object reference, usable from any thread.
    unsafe impl Send for Process {}

    impl Process {
        pub fn raw(&self) -> HANDLE {
            self.0
        }

        /// Whether it ended within `patience`.
        pub fn wait(&self, patience: Duration) -> bool {
            let ms = u32::try_from(patience.as_millis()).unwrap_or(u32::MAX);
            // SAFETY: the handle is open for as long as `self` lives.
            unsafe { WaitForSingleObject(self.0, ms) == WAIT_OBJECT_0 }
        }
    }

    impl Drop for Process {
        fn drop(&mut self) {
            // SAFETY: the handle came from ShellExecuteExW and is closed exactly once, here.
            unsafe { CloseHandle(self.0) };
        }
    }

    fn wide(text: &str) -> Vec<u16> {
        text.encode_utf16().chain([0]).collect()
    }

    /// Runs `exe params` with `verb` ("runas": elevated, after the UAC prompt), its console
    /// hidden. Blocks while the prompt is up. The user declining is `ERROR_CANCELLED` (1223).
    pub fn elevated(exe: &Path, params: &str, verb: &str) -> io::Result<Process> {
        let (verb, file, params) = (wide(verb), wide(&exe.to_string_lossy()), wide(params));
        // SAFETY: SHELLEXECUTEINFOW is plain data; all-zero is its documented empty state.
        let mut info: SHELLEXECUTEINFOW = unsafe { std::mem::zeroed() };
        info.cbSize = size_of::<SHELLEXECUTEINFOW>() as u32;
        info.fMask = SEE_MASK_NOCLOSEPROCESS | SEE_MASK_FLAG_NO_UI | SEE_MASK_NOASYNC;
        info.lpVerb = verb.as_ptr();
        info.lpFile = file.as_ptr();
        info.lpParameters = params.as_ptr();
        info.nShow = SW_HIDE;
        // ShellExecuteEx may hand the verb to shell extensions, which want COM on this thread.
        // SAFETY: no pointers but the reserved null; balanced by CoUninitialize below.
        let com = unsafe {
            CoInitializeEx(
                std::ptr::null(),
                (COINIT_APARTMENTTHREADED | COINIT_DISABLE_OLE1DDE) as u32,
            )
        };
        // SAFETY: `info` and the strings it points to outlive the call.
        let ok = unsafe { ShellExecuteExW(&mut info) };
        let error = io::Error::last_os_error();
        if com >= 0 {
            // SAFETY: pairs with the successful CoInitializeEx above, on the same thread.
            unsafe { CoUninitialize() };
        }
        if ok == 0 {
            return Err(error);
        }
        if info.hProcess.is_null() {
            return Err(io::Error::other("Windows gave no handle to the started process"));
        }
        Ok(Process(info.hProcess))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn declining_uac_means_needs_permission_and_anything_else_is_an_error() {
        assert_eq!(launch_failure(&io::Error::from_raw_os_error(1223)), State::NeedsPermission);
        assert!(matches!(
            launch_failure(&io::Error::from_raw_os_error(2)),
            State::Failed(text) if text.contains("Couldn't start")
        ));
    }

    /// ShellExecuteEx's real error path (no UAC: verb "open" on a file that doesn't exist).
    #[cfg(windows)]
    #[test]
    fn a_launch_that_fails_reports_the_os_error() {
        let error = launch::elevated(Path::new(r"C:\no\such\sonicnet-system.exe"), "", "open")
            .expect_err("can't start what isn't there");
        assert_eq!(error.raw_os_error(), Some(2), "{error}");
        assert!(matches!(launch_failure(&error), State::Failed(_)));
    }

    /// A launch that works hands back a handle we can wait on.
    #[cfg(windows)]
    #[test]
    fn a_launched_process_can_be_waited_for() {
        let cmd = std::path::PathBuf::from(std::env::var("ComSpec").unwrap_or("cmd.exe".into()));
        let process = launch::elevated(&cmd, "/c exit", "open").expect("cmd starts");
        assert!(process.wait(Duration::from_secs(10)));
    }

    /// Off → Disable publishes Off; a shutdown with nothing running answers at once.
    #[test]
    fn supervisor_without_a_helper() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let (to_ui, mut from) = mpsc::unbounded_channel();
        let requests = spawn(runtime.handle(), to_ui);
        requests.send(Request::Disable).unwrap();
        let got = runtime.block_on(async { from.recv().await });
        assert_eq!(got, Some(Shell::System(Status::default())));
        let started = std::time::Instant::now();
        runtime.block_on(shutdown(&requests));
        assert!(started.elapsed() < Duration::from_secs(1));
    }
}
