use std::io::Write;
use std::sync::{Arc, Mutex};

use indicatif::ProgressBar;
use tracing_subscriber::fmt::MakeWriter;

/// Global progress bar state shared between the syncer and tracing writer.
static PROGRESS_BAR: std::sync::OnceLock<Arc<Mutex<Option<ProgressBar>>>> = std::sync::OnceLock::new();

fn global_pb() -> &'static Arc<Mutex<Option<ProgressBar>>> {
    PROGRESS_BAR.get_or_init(|| Arc::new(Mutex::new(None)))
}

/// Set the active progress bar. All tracing output will be routed through pb.println().
pub fn set_progress_bar(pb: ProgressBar) {
    *global_pb().lock().unwrap() = Some(pb);
}

/// Clear the active progress bar. Tracing output returns to normal stderr.
pub fn clear_progress_bar() {
    *global_pb().lock().unwrap() = None;
}

/// A tracing writer that routes output through the active progress bar.
/// When no progress bar is active, writes directly to stderr.
pub struct ProgressWriter;

impl Write for ProgressWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let pb = global_pb().lock().unwrap().clone();
        if let Some(pb) = pb {
            let s = String::from_utf8_lossy(buf);
            // pb.println() adds a newline, so strip trailing newline from tracing output
            let s = s.trim_end_matches('\n');
            if !s.is_empty() {
                pb.println(s);
            }
        } else {
            std::io::stderr().write_all(buf)?;
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        std::io::stderr().flush()
    }
}

/// MakeWriter impl for tracing-subscriber integration.
#[derive(Clone)]
pub struct ProgressMakeWriter;

impl<'a> MakeWriter<'a> for ProgressMakeWriter {
    type Writer = ProgressWriter;

    fn make_writer(&'a self) -> Self::Writer {
        ProgressWriter
    }
}
