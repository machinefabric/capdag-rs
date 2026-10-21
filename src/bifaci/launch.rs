//! How a cartridge entry is started.
//!
//! A scaffolded Python cartridge is `cartridge.py`, and on Unix its shebang
//! makes it directly executable. Windows has no shebang: `CreateProcess` — and
//! so every language's `exec` — refuses the file outright with
//!
//! ```text
//! %1 is not a valid Win32 application
//! ```
//!
//! `capdag dev-install` therefore could not read a Python project's manifest,
//! and no scaffolded Python cartridge could be launched on the platform at all.
//! Naming the interpreter is what the shebang was doing; doing it here does it
//! on both platforms.
//!
//! One module, because starting a cartridge happens in three places — reading a
//! manifest, probing its caps, hosting it — and each of them wrote its own
//! `Command::new(entry)`. All three were wrong in the same way at once, which
//! is what having three of them buys.

use std::path::Path;

/// How an entry that is a SCRIPT is run, by extension.
///
/// Keyed on the extension rather than on the language, because the callers that
/// need it have a PATH and not a language: `project_entry` finds an entry by
/// looking, and what it finds is a filename.
const INTERPRETERS: &[(&str, &str)] = &[("py", "python3"), ("js", "node")];

/// What a COMPILED entry is called on this platform.
///
/// A scaffolded Rust cartridge declares `target/release/<name>` and Cargo
/// writes `target/release/<name>.exe`. Looking for the declared spelling found
/// nothing on Windows, so a project that had built perfectly reported that it
/// had no entry.
pub fn executable_suffix() -> &'static str {
    if cfg!(target_os = "windows") {
        ".exe"
    } else {
        ""
    }
}

/// The program that runs `entry`, and the arguments that precede the entry's
/// own.
///
/// A compiled entry runs itself. A script entry runs under the interpreter its
/// extension names.
pub fn launcher(entry: &Path) -> (String, Vec<String>) {
    let extension = entry
        .extension()
        .map(|e| e.to_string_lossy().to_lowercase())
        .unwrap_or_default();
    let Some((_, interpreter)) = INTERPRETERS.iter().find(|(name, _)| *name == extension) else {
        return (entry.display().to_string(), Vec::new());
    };
    let entry = entry.display().to_string();
    // `python3` is the name everywhere except a Windows install, which ships
    // `python.exe` and no `python3.exe`. Asked in order, and the entry itself
    // is never the answer here: a machine with no interpreter gets a refusal
    // naming the interpreter rather than one naming the file.
    if cfg!(target_os = "windows") && *interpreter == "python3" {
        for candidate in ["python3", "python"] {
            if on_path(candidate) {
                return (candidate.to_string(), vec![entry]);
            }
        }
    }
    ((*interpreter).to_string(), vec![entry])
}

/// Whether a bare program name resolves on this machine.
///
/// `PATHEXT` is consulted on Windows, because `python` there is `python.exe`
/// and a search for the bare name finds nothing.
fn on_path(program: &str) -> bool {
    let Some(path) = std::env::var_os("PATH") else {
        return false;
    };
    let extensions: Vec<String> = if cfg!(target_os = "windows") {
        std::env::var("PATHEXT")
            .unwrap_or_else(|_| ".COM;.EXE;.BAT;.CMD".to_string())
            .split(';')
            .filter(|e| !e.is_empty())
            .map(|e| e.to_lowercase())
            .collect()
    } else {
        vec![String::new()]
    };
    std::env::split_paths(&path).any(|directory| {
        extensions
            .iter()
            .any(|extension| directory.join(format!("{program}{extension}")).is_file())
    })
}

/// A command that runs a cartridge entry.
pub fn command(entry: &Path) -> std::process::Command {
    let (program, leading) = launcher(entry);
    let mut command = std::process::Command::new(program);
    command.args(leading);
    command
}

/// The same, for the async host.
pub fn tokio_command(entry: &Path) -> tokio::process::Command {
    let (program, leading) = launcher(entry);
    let mut command = tokio::process::Command::new(program);
    command.args(leading);
    command
}

/// How much of what a probed cartridge writes on stderr is kept: the end of
/// it, where a crash or a refusal says why.
pub const PROBE_STDERR_TAIL: usize = 4096;

/// Drain a started cartridge's stderr and keep its last
/// [`PROBE_STDERR_TAIL`] bytes, for a start that only asks the cartridge who
/// it is (its manifest) and stops it again.
///
/// Such a start never lets the cartridge write on the caller's own stderr:
/// whoever ran the command owns that stream — a terminal interface drawing
/// on it, a pipe being parsed — and a cartridge's start-up chatter has no
/// place in it. What it wrote is not thrown away either: a probe that fails
/// says what the cartridge said ([`with_stderr`]). The pipe is drained to its
/// end, so a cartridge that writes a lot is never blocked on it; the task
/// ends when the process does.
pub fn drain_stderr_tail(
    stderr: tokio::process::ChildStderr,
) -> tokio::task::JoinHandle<Vec<u8>> {
    tokio::spawn(async move {
        use tokio::io::AsyncReadExt;
        let mut stderr = stderr;
        let mut tail: Vec<u8> = Vec::new();
        let mut buffer = [0u8; 4096];
        loop {
            match stderr.read(&mut buffer).await {
                Ok(0) | Err(_) => break,
                Ok(read) => {
                    tail.extend_from_slice(&buffer[..read]);
                    if tail.len() > PROBE_STDERR_TAIL {
                        tail.drain(..tail.len() - PROBE_STDERR_TAIL);
                    }
                }
            }
        }
        tail
    })
}

/// `message`, followed by what the cartridge wrote on stderr when it wrote
/// anything. Waits for the drain to end — call it once the process has been
/// stopped, or it waits for the process to end.
pub async fn with_stderr(
    message: String,
    tail: tokio::task::JoinHandle<Vec<u8>>,
) -> String {
    match tail.await {
        Ok(bytes) if bytes.iter().any(|byte| !byte.is_ascii_whitespace()) => {
            format!(
                "{message}\n--- cartridge stderr (last {} bytes) ---\n{}",
                bytes.len(),
                String::from_utf8_lossy(&bytes).trim_end()
            )
        }
        Ok(_) => message,
        Err(join) => format!("{message}\n(its stderr could not be read: {join})"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    /// TEST7162: a script cartridge is started through its interpreter.
    #[test]
    fn test7162_a_script_entry_is_launched_through_an_interpreter() {
        let entry = PathBuf::from("proj").join("cartridge.py");
        let (program, leading) = launcher(&entry);
        assert_ne!(
            program,
            entry.display().to_string(),
            "a .py must not be launched as a program"
        );
        assert_eq!(leading, vec![entry.display().to_string()]);

        // Case does not decide it.
        let (program, _) = launcher(Path::new("CARTRIDGE.PY"));
        assert_ne!(program, "CARTRIDGE.PY", "extension matching is case-blind");
    }

    /// TEST7163: a compiled cartridge is started as itself.
    ///
    /// The rule keys on the extension, so it has to leave alone the entries
    /// that already are programs. Running a Rust cartridge's binary through an
    /// interpreter would be a new failure invented by the fix.
    #[test]
    fn test7163_a_compiled_entry_runs_itself() {
        let entry = PathBuf::from("target")
            .join("release")
            .join(format!("mood-tagger{}", executable_suffix()));
        let (program, leading) = launcher(&entry);
        assert_eq!(program, entry.display().to_string());
        assert!(leading.is_empty(), "a compiled entry takes no leading arguments");
    }

    /// TEST7164: a compiled entry carries the platform's suffix.
    ///
    /// The stub declares `target/release/<name>` — one string, vendored into
    /// four mirrors, so it cannot carry one platform's spelling. Cargo writes
    /// `<name>.exe` on Windows, and looking for the declared spelling found
    /// nothing: a project that had built perfectly reported that it had no
    /// entry.
    #[test]
    fn test7164_a_compiled_entry_carries_the_platforms_suffix() {
        if cfg!(target_os = "windows") {
            assert_eq!(executable_suffix(), ".exe");
        } else {
            assert_eq!(executable_suffix(), "");
        }
    }

    /// TEST7165: the entry's own arguments come after the interpreter's.
    ///
    /// `command(entry).arg("manifest")` has to produce
    /// `python3 cartridge.py manifest` and never
    /// `python3 manifest cartridge.py`, which would ask the interpreter to run
    /// a file called `manifest`.
    #[test]
    fn test7165_the_entrys_arguments_follow_it() {
        let entry = PathBuf::from("proj").join("cartridge.py");
        let mut built = command(&entry);
        built.arg("manifest");
        let argv: Vec<String> = built
            .get_args()
            .map(|a| a.to_string_lossy().to_string())
            .collect();
        assert_eq!(argv.last().map(String::as_str), Some("manifest"));
        assert!(
            argv[argv.len() - 2].ends_with("cartridge.py"),
            "the entry must precede its own arguments: {argv:?}"
        );
    }

    /// TEST12165: nothing starts a process except through here.
    ///
    /// Starting a cartridge happened in four places, each writing its own
    /// `Command::new(entry)`. Three were routed through this module; the
    /// orchestrator's manifest discovery was not, and a Python dev cartridge
    /// failed there on Windows with "%1 is not a valid Win32 application" while
    /// every other start of it worked. A fifth would be missed the same way, so
    /// the source is read for one.
    #[test]
    fn test12165_every_process_start_goes_through_the_launcher() {
        let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
        let mut found = Vec::new();
        let mut stack = vec![src];
        while let Some(dir) = stack.pop() {
            for entry in std::fs::read_dir(&dir).expect("reading src") {
                let path = entry.expect("an entry").path();
                if path.is_dir() {
                    stack.push(path);
                    continue;
                }
                if path.extension().and_then(|e| e.to_str()) != Some("rs")
                    || path.ends_with("launch.rs")
                {
                    continue;
                }
                let text = std::fs::read_to_string(&path).expect("reading a source file");
                for (number, line) in text.lines().enumerate() {
                    let code = line.split("//").next().unwrap_or("");
                    if code.contains("Command::new(") {
                        found.push(format!("{}:{}", path.display(), number + 1));
                    }
                }
            }
        }
        assert!(
            found.is_empty(),
            "a process is started outside bifaci::launch, so a script entry \
             cannot run there on Windows: {found:?}"
        );
    }
}
