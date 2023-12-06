// Copyright (c) 2019 Ant Financial
//
// SPDX-License-Identifier: Apache-2.0
//

use anyhow::{anyhow, Result, Context};
use nix::fcntl::{self, OFlag};
use nix::mount::MsFlags;
use nix::sched::{unshare, CloneFlags};
use nix::sys::stat::Mode;
use nix::sys::wait::wait;
use nix::unistd::{fork, ForkResult, getpid, gettid, self, Uid, Gid};
use oci::LinuxIdMapping;
use protocols::oci::LinuxIDMapping;
use protocols::trans::from_vec;
use slog::Logger;
use std::fmt;
use std::fs;
use std::fs::File;
use std::path::{Path, PathBuf};
use tracing::instrument;
use rustjail::sync::{read_sync, write_sync, SYNC_SUCCESS};
use crate::mount::baremount;

const PERSISTENT_NS_DIR: &str = "/var/run/sandbox-ns";
pub const NSTYPEIPC: &str = "ipc";
pub const NSTYPEUTS: &str = "uts";
pub const NSTYPEPID: &str = "pid";
pub const NSTYPEUSER: &str = "user";
pub const NSTYPENET: &str = "network";

#[instrument]
fn get_proc_ns_path(pid: i32, ns_type: &str) -> String {
    format!("/proc/{}/ns/{}", pid, ns_type)
}

#[instrument]
fn get_current_thread_ns_path(ns_type: &str) -> String {
    format!("/proc/{}/task/{}/ns/{}", getpid(), gettid(), ns_type)
}

#[instrument]
fn get_proc_id_map_path(pid: i32, is_uid: bool) -> String {
    if is_uid {
        format!("/proc/{}/uid_map", pid)
    } else {
        format!("/proc/{}/gid_map", pid)
    }
}

#[instrument]
fn persist_namespace(ns_type: NamespaceType, hostname: Option<&String>, source: &Path, destination: &Path, logger: &Logger) -> Result<()> {
    unshare(ns_type.get_flags())?;
    if ns_type == NamespaceType::Uts && hostname.is_some() {
        nix::unistd::sethostname(hostname.unwrap())?;
    }
    // Bind mount the new namespace from the current thread onto the mount point to persist it.

    let mut flags = MsFlags::empty();
    flags |= MsFlags::MS_BIND | MsFlags::MS_REC;

    baremount(source, destination, "none", flags, "", &logger).map_err(|e| {
        anyhow!(
            "Failed to mount {:?} to {:?} with err:{:?}",
            source,
            destination,
            e
        )
    })?;
    Ok(())
}

#[instrument]
fn setid(uid: Uid, gid: Gid) -> Result<()> {
    unistd::setresuid(uid, uid, uid).map_err(|e| anyhow!(e).context("setresuid failed"))?;
    unistd::setresgid(gid, gid, gid).map_err(|e| anyhow!(e).context("setresgid failed"))?;

    Ok(())
}

#[instrument]
fn write_mappings(logger: &Logger, path: &str, maps: &[LinuxIdMapping]) -> Result<()> {
    let data = maps
        .iter()
        .filter(|m| m.size != 0)
        .map(|m| format!("{} {} {}\n", m.container_id, m.host_id, m.size))
        .collect::<Vec<_>>()
        .join("");

    info!(logger, "mapping: {}", data);
    if !data.is_empty() {
        let fd = fcntl::open(path, OFlag::O_WRONLY, Mode::empty())?;
        defer!(unistd::close(fd).unwrap());
        unistd::write(fd, data.as_bytes()).map_err(|e| {
            info!(logger, "cannot write mapping");
            e
        })?;
    }
    Ok(())
}

#[derive(Debug)]
pub struct Namespace {
    logger: Logger,
    pub path: String,
    persistent_ns_dir: String,
    ns_type: NamespaceType,
    //only used for uts namespace
    pub hostname: Option<String>,
    pub uid_mappings: Option<Vec<LinuxIDMapping>>,
    pub gid_mappings: Option<Vec<LinuxIDMapping>>,
}

impl Namespace {
    #[instrument]
    pub fn new(logger: &Logger) -> Self {
        Namespace {
            logger: logger.clone(),
            path: String::from(""),
            persistent_ns_dir: String::from(PERSISTENT_NS_DIR),
            ns_type: NamespaceType::Ipc,
            hostname: None,
            uid_mappings: None,
            gid_mappings: None,
        }
    }

    #[instrument]
    pub fn get_ipc(mut self) -> Self {
        self.ns_type = NamespaceType::Ipc;
        self
    }

    #[instrument]
    pub fn get_uts(mut self, hostname: &str) -> Self {
        self.ns_type = NamespaceType::Uts;
        if !hostname.is_empty() {
            self.hostname = Some(String::from(hostname));
        }
        self
    }

    #[instrument]
    pub fn get_pid(mut self) -> Self {
        self.ns_type = NamespaceType::Pid;
        self
    }

    #[instrument]
    pub fn get_user(mut self, uid_mappings: Vec<LinuxIDMapping>, gid_mappings: Vec<LinuxIDMapping>) -> Self {
        self.ns_type = NamespaceType::User;
        if !uid_mappings.is_empty() {
            self.uid_mappings = Some(uid_mappings);
        }
        if !gid_mappings.is_empty() {
            self.gid_mappings = Some(gid_mappings);
        }

        self
    }

    #[instrument]
    pub fn get_net(mut self) -> Self {
        self.ns_type = NamespaceType::Net;
        self
    }

    #[allow(dead_code)]
    pub fn set_root_dir(mut self, dir: &str) -> Self {
        self.persistent_ns_dir = dir.to_string();
        self
    }

    // setup creates persistent namespace without switching to it.
    // Note, pid namespaces cannot be persisted.
    #[instrument]
    #[allow(clippy::question_mark)]
    pub async fn setup(mut self) -> Result<Self> {
        fs::create_dir_all(&self.persistent_ns_dir)?;

        let ns_path = PathBuf::from(&self.persistent_ns_dir);
        let ns_type = self.ns_type;
        if ns_type == NamespaceType::Pid {
            return Err(anyhow!("Cannot persist namespace of PID type"));
        }
        let logger = self.logger.clone();

        let new_ns_path = ns_path.join(ns_type.get());

        File::create(new_ns_path.as_path())?;

        self.path = new_ns_path.clone().into_os_string().into_string().unwrap();
        let hostname = self.hostname.clone();

        let new_thread = std::thread::spawn(move || {
            if let Err(err) = || -> Result<()> {
                let origin_ns_path = get_current_thread_ns_path(ns_type.get());

                let source = Path::new(&origin_ns_path);
                let destination = new_ns_path.as_path();

                File::open(source)?;

                // Create a new netns on the current thread.
                let cf = ns_type.get_flags();

                unshare(cf)?;

                if ns_type == NamespaceType::Uts && hostname.is_some() {
                    nix::unistd::sethostname(hostname.unwrap())?;
                }
                // Bind mount the new namespace from the current thread onto the mount point to persist it.

                let mut flags = MsFlags::empty();
                flags |= MsFlags::MS_BIND | MsFlags::MS_REC;

                baremount(source, destination, "none", flags, "", &logger).map_err(|e| {
                    anyhow!(
                        "Failed to mount {:?} to {:?} with err:{:?}",
                        source,
                        destination,
                        e
                    )
                })?;

                Ok(())
            }() {
                return Err(err);
            }

            Ok(())
        });

        new_thread
            .join()
            .map_err(|e| anyhow!("Failed to join thread {:?}!", e))??;

        Ok(self)
    }
}

/// setup persistent namespace in a non-root user namespace without switching to it.
/// Note, pid namespaces cannot be persisted.
///
/// CLONE_NEWUSER requires that the calling process is not threaded,
/// so use a child process to execute unshare() or setns().
/// Ref: https://man7.org/linux/man-pages/man2/unshare.2.html
#[instrument]
#[allow(clippy::question_mark)]
pub async fn setup_in_userns(
    logger: &Logger,
    userns: &mut Namespace,
    nses: Vec<&mut Namespace>,
) -> Result<()> {
    let (prfd, cwfd) = unistd::pipe().context("failed to create pipe")?;
    let (crfd, pwfd) = unistd::pipe().context("failed to create pipe")?;

    match unsafe { fork() } {
        Ok(ForkResult::Parent { child, .. }) => {
            unistd::close(crfd)?;
            unistd::close(cwfd)?;

            let mut mounts = Vec::new();
            let logger = logger.clone();

            // create mount path of namespaces
            fs::create_dir_all(&userns.persistent_ns_dir)?;
            let ns_path = PathBuf::from(&userns.persistent_ns_dir);
            let new_ns_path = ns_path.join(userns.ns_type.get());
            File::create(new_ns_path.as_path())?;
            userns.path = new_ns_path.clone().into_os_string().into_string().unwrap();
            let origin_ns_path = get_proc_ns_path(child.as_raw(), userns.ns_type.get());
            mounts.push((origin_ns_path.clone(), new_ns_path.clone()));

            for ns in nses {
                let ns_type = ns.ns_type;
                if ns_type == NamespaceType::Pid {
                    return Err(anyhow!("Cannot persist namespace of PID type"));
                }

                fs::create_dir_all(&ns.persistent_ns_dir)?;

                let ns_path = PathBuf::from(&ns.persistent_ns_dir);
                let new_ns_path = ns_path.join(ns_type.get());
                File::create(new_ns_path.as_path())?;
                ns.path = new_ns_path.clone().into_os_string().into_string().unwrap();

                let origin_ns_path = get_proc_ns_path(child.as_raw(), ns_type.get());

                mounts.push((origin_ns_path, new_ns_path));
            }

            // wait child to setup user namespace
            read_sync(prfd)?;

            // after creating the userns, remap ids (temporarily remap 0~65535 to 1~65536).
            if let Some(uid_mappings) = userns.uid_mappings.clone() {
                write_mappings(&logger, &get_proc_id_map_path(child.as_raw(), true), &from_vec(uid_mappings)).map_err(|e| anyhow!(e).context("parent write child's uidmappings failed"))?;
            }
            if let Some(gid_mappings) = userns.gid_mappings.clone() {
                write_mappings(&logger, &get_proc_id_map_path(child.as_raw(), false), &from_vec(gid_mappings)).map_err(|e| anyhow!(e).context("parent write child's gidmappings failed"))?;
            }

            // notify child to continue
            write_sync(pwfd, SYNC_SUCCESS, "")?;

            // wait child to setup other namespaces
            read_sync(prfd)?;

            // Bind mount the new namespaces onto the mount point to persist it.
            for (origin_ns_path, new_ns_path) in mounts {
                let source = Path::new(&origin_ns_path);
                let destination = new_ns_path.as_path();

                File::open(source)?;

                let mut flags = MsFlags::empty();
                flags |= MsFlags::MS_BIND | MsFlags::MS_REC;

                baremount(source, destination, "none", flags, "", &logger).map_err(|e| {
                    anyhow!(
                        "Failed to mount {:?} to {:?} with err:{:?}",
                        source,
                        destination,
                        e
                    )
                })?;
            }

            // notify child to exit
            write_sync(pwfd, SYNC_SUCCESS, "")?;

            unistd::close(prfd)?;
            unistd::close(pwfd)?;
            wait()?;
        },
        Ok(ForkResult::Child) => {
            unistd::close(prfd)?;
            unistd::close(pwfd)?;

            let cf = userns.ns_type.get_flags();
            unshare(cf).map_err(|e| anyhow!(e).context("child unshare user ns failed"))?;

            // notify parent user namespace creation is complete
            write_sync(cwfd, SYNC_SUCCESS, "")?;

            // wait parent to remap ids
            read_sync(crfd)?;

            setid(Uid::from_raw(0), Gid::from_raw(0))?;

            for ns in nses {
                let cf = ns.ns_type.get_flags();
                unshare(cf).map_err(|e| anyhow!(e).context(format!("child unshare {} ns failed", ns.ns_type.get())))?;

                let hostname = ns.hostname.clone();
                if ns.ns_type == NamespaceType::Uts && hostname.is_some() {
                    nix::unistd::sethostname(hostname.unwrap())?;
                }
            }

            // notify parent to persist namespace
            write_sync(cwfd, SYNC_SUCCESS, "")?;

            // wait parent to persist namespace
            read_sync(crfd)?;

            std::process::exit(0);
        }
        Err(e) => {
            return Err(anyhow!(format!(
                "failed to fork namespace setup process: {:?}",
                e
            )));
        }
    };

    Ok(())
}

/// Represents the Namespace type.
#[derive(Clone, Copy, PartialEq)]
enum NamespaceType {
    Ipc,
    Uts,
    Pid,
    User,
    Net,
}

impl NamespaceType {
    /// Get the string representation of the namespace type.
    pub fn get(&self) -> &str {
        match *self {
            Self::Ipc => "ipc",
            Self::Uts => "uts",
            Self::Pid => "pid",
            Self::User => "user",
            Self::Net => "net",
        }
    }

    /// Get the associate flags with the namespace type.
    pub fn get_flags(&self) -> CloneFlags {
        match *self {
            Self::Ipc => CloneFlags::CLONE_NEWIPC,
            Self::Uts => CloneFlags::CLONE_NEWUTS,
            Self::Pid => CloneFlags::CLONE_NEWPID,
            Self::User => CloneFlags::CLONE_NEWUSER,
            Self::Net => CloneFlags::CLONE_NEWNET,
        }
    }
}

impl fmt::Debug for NamespaceType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.get())
    }
}

#[cfg(test)]
mod tests {
    use super::{Namespace, NamespaceType};
    use crate::mount::remove_mounts;
    use nix::sched::CloneFlags;
    use tempfile::Builder;
    use test_utils::skip_if_not_root;

    #[tokio::test]
    async fn test_setup_persistent_ns() {
        skip_if_not_root!();
        // Create dummy logger and temp folder.
        let logger = slog::Logger::root(slog::Discard, o!());
        let tmpdir = Builder::new().prefix("ipc").tempdir().unwrap();

        let ns_ipc = Namespace::new(&logger)
            .get_ipc()
            .set_root_dir(tmpdir.path().to_str().unwrap())
            .setup()
            .await;

        assert!(ns_ipc.is_ok());
        assert!(remove_mounts(&[ns_ipc.unwrap().path]).is_ok());

        let logger = slog::Logger::root(slog::Discard, o!());
        let tmpdir = Builder::new().prefix("uts").tempdir().unwrap();

        let ns_uts = Namespace::new(&logger)
            .get_uts("test_hostname")
            .set_root_dir(tmpdir.path().to_str().unwrap())
            .setup()
            .await;

        assert!(ns_uts.is_ok());
        assert!(remove_mounts(&[ns_uts.unwrap().path]).is_ok());

        // Check it cannot persist pid namespaces.
        let logger = slog::Logger::root(slog::Discard, o!());
        let tmpdir = Builder::new().prefix("pid").tempdir().unwrap();

        let ns_pid = Namespace::new(&logger)
            .get_pid()
            .set_root_dir(tmpdir.path().to_str().unwrap())
            .setup()
            .await;

        assert!(ns_pid.is_err());
    }

    #[test]
    fn test_namespace_type() {
        let ipc = NamespaceType::Ipc;
        assert_eq!("ipc", ipc.get());
        assert_eq!(CloneFlags::CLONE_NEWIPC, ipc.get_flags());

        let uts = NamespaceType::Uts;
        assert_eq!("uts", uts.get());
        assert_eq!(CloneFlags::CLONE_NEWUTS, uts.get_flags());

        let pid = NamespaceType::Pid;
        assert_eq!("pid", pid.get());
        assert_eq!(CloneFlags::CLONE_NEWPID, pid.get_flags());
    }

    #[test]
    fn test_new() {
        // Create dummy logger and temp folder.
        let logger = slog::Logger::root(slog::Discard, o!());

        let ns_ipc = Namespace::new(&logger);
        assert_eq!(NamespaceType::Ipc, ns_ipc.ns_type);
    }

    #[test]
    fn test_get_ipc() {
        // Create dummy logger and temp folder.
        let logger = slog::Logger::root(slog::Discard, o!());

        let ns_ipc = Namespace::new(&logger).get_ipc();
        assert_eq!(NamespaceType::Ipc, ns_ipc.ns_type);
    }

    #[test]
    fn test_get_uts_with_hostname() {
        let hostname = String::from("a.test.com");
        // Create dummy logger and temp folder.
        let logger = slog::Logger::root(slog::Discard, o!());

        let ns_uts = Namespace::new(&logger).get_uts(hostname.as_str());
        assert_eq!(NamespaceType::Uts, ns_uts.ns_type);
        assert!(ns_uts.hostname.is_some());
    }

    #[test]
    fn test_get_uts() {
        let hostname = String::from("");
        // Create dummy logger and temp folder.
        let logger = slog::Logger::root(slog::Discard, o!());

        let ns_uts = Namespace::new(&logger).get_uts(hostname.as_str());
        assert_eq!(NamespaceType::Uts, ns_uts.ns_type);
        assert!(ns_uts.hostname.is_none());
    }

    #[test]
    fn test_get_pid() {
        // Create dummy logger and temp folder.
        let logger = slog::Logger::root(slog::Discard, o!());

        let ns_pid = Namespace::new(&logger).get_pid();
        assert_eq!(NamespaceType::Pid, ns_pid.ns_type);
    }

    #[test]
    fn test_set_root_dir() {
        // Create dummy logger and temp folder.
        let logger = slog::Logger::root(slog::Discard, o!());
        let tmpdir = Builder::new().prefix("pid").tempdir().unwrap();

        let ns_root = Namespace::new(&logger).set_root_dir(tmpdir.path().to_str().unwrap());
        assert_eq!(NamespaceType::Ipc, ns_root.ns_type);
        assert_eq!(ns_root.persistent_ns_dir, tmpdir.path().to_str().unwrap());
    }

    #[test]
    fn test_namespace_type_get() {
        #[derive(Debug)]
        struct TestData<'a> {
            ns_type: NamespaceType,
            str: &'a str,
        }

        let tests = &[
            TestData {
                ns_type: NamespaceType::Ipc,
                str: "ipc",
            },
            TestData {
                ns_type: NamespaceType::Uts,
                str: "uts",
            },
            TestData {
                ns_type: NamespaceType::Pid,
                str: "pid",
            },
        ];

        // Run the tests
        for (i, d) in tests.iter().enumerate() {
            // Create a string containing details of the test
            let msg = format!("test[{}]: {:?}", i, d);
            assert_eq!(d.str, d.ns_type.get(), "{}", msg)
        }
    }

    #[test]
    fn test_namespace_type_get_flags() {
        #[derive(Debug)]
        struct TestData {
            ns_type: NamespaceType,
            ns_flag: CloneFlags,
        }

        let tests = &[
            TestData {
                ns_type: NamespaceType::Ipc,
                ns_flag: CloneFlags::CLONE_NEWIPC,
            },
            TestData {
                ns_type: NamespaceType::Uts,
                ns_flag: CloneFlags::CLONE_NEWUTS,
            },
            TestData {
                ns_type: NamespaceType::Pid,
                ns_flag: CloneFlags::CLONE_NEWPID,
            },
        ];

        // Run the tests
        for (i, d) in tests.iter().enumerate() {
            // Create a string containing details of the test
            let msg = format!("test[{}]: {:?}", i, d);
            assert_eq!(d.ns_flag, d.ns_type.get_flags(), "{}", msg)
        }
    }
}
