// Copyright (c) 2019-2022 Alibaba Cloud
// Copyright (c) 2019-2022 Ant Group
//
// SPDX-License-Identifier: Apache-2.0
//

use std::collections::HashMap;
use std::sync::Arc;

use agent::Agent;
use anyhow::{anyhow, Context, Result};
use common::{
    error::Error,
    types::{
        ContainerConfig, ContainerID, ContainerProcess, ProcessStateInfo, ProcessStatus,
        ProcessType,
    },
};
use kata_sys_util::k8s::update_ephemeral_storage_type;
use kata_types::{
    annotations::{BUNDLE_PATH_KEY, CONTAINER_TYPE_KEY, KATA_ANNO_CFG_HYPERVISOR_INIT_DATA},
    config::TomlConfig,
    container::{update_ocispec_annotations, POD_CONTAINER, POD_SANDBOX},
    k8s::{self, container_type},
};
use oci_spec::runtime as oci;

use oci::{LinuxResources, Process as OCIProcess};
use resource::{
    cdi_devices::container_device::annotate_container_devices, ResourceManager, ResourceUpdateOp,
};
use tokio::sync::RwLock;

use super::{
    process::{Process, ProcessWatcher},
    ContainerInner,
};
use crate::container_manager::{is_termination_signal, logger_with_process};

pub struct Exec {
    pub(crate) process: Process,
    pub(crate) oci_process: OCIProcess,
}

pub struct Container {
    pid: u32,
    pub container_id: ContainerID,
    config: ContainerConfig,
    spec: oci::Spec,
    inner: Arc<RwLock<ContainerInner>>,
    agent: Arc<dyn Agent>,
    resource_manager: Arc<ResourceManager>,
    logger: slog::Logger,
    pub(crate) passfd_listener_addr: Option<(String, u32)>,
}

impl Container {
    pub async fn new(
        pid: u32,
        config: ContainerConfig,
        spec: oci::Spec,
        agent: Arc<dyn Agent>,
        resource_manager: Arc<ResourceManager>,
        passfd_listener_addr: Option<(String, u32)>,
    ) -> Result<Self> {
        let container_id = ContainerID::new(&config.container_id).context("new container id")?;
        let logger = sl!().new(o!("container_id" => config.container_id.clone()));
        let process = ContainerProcess::new(&config.container_id, "")?;
        let init_process = Process::new(
            &process,
            pid,
            &config.bundle,
            config.stdin.clone(),
            config.stdout.clone(),
            config.stderr.clone(),
            config.terminal,
        );
        let linux_resources = spec
            .linux()
            .as_ref()
            .and_then(|linux| linux.resources().clone());

        Ok(Self {
            pid,
            container_id,
            config,
            spec,
            inner: Arc::new(RwLock::new(ContainerInner::new(
                agent.clone(),
                init_process,
                logger.clone(),
                linux_resources,
            ))),
            agent,
            resource_manager,
            logger,
            passfd_listener_addr,
        })
    }

    pub async fn create(&self, mut spec: oci::Spec) -> Result<()> {
        // process oci spec
        let mut inner = self.inner.write().await;
        let toml_config = self.resource_manager.config().await;
        let config = &self.config;
        let sandbox_pidns = is_pid_namespace_enabled(&spec);
        let disable_guest_selinux = get_disable_guest_selinux(&toml_config);
        let annotations = spec.annotations().clone().unwrap_or_default();
        let container_typ = container_type(&spec);
        let pod_type_anno = if container_typ.is_pod_container() {
            (CONTAINER_TYPE_KEY.to_string(), POD_CONTAINER.to_string())
        } else {
            (CONTAINER_TYPE_KEY.to_string(), POD_SANDBOX.to_string())
        };

        let bund_path_anno = (BUNDLE_PATH_KEY.to_string(), config.bundle.clone());
        let updated_annotations = update_ocispec_annotations(
            &annotations,
            &[KATA_ANNO_CFG_HYPERVISOR_INIT_DATA],
            &[pod_type_anno, bund_path_anno],
        );
        spec.set_annotations(Some(updated_annotations.clone()));

        amend_spec(
            &mut spec,
            toml_config.runtime.disable_guest_seccomp,
            disable_guest_selinux,
            toml_config.runtime.disable_guest_empty_dir,
            &toml_config.runtime.emptydir_mode,
        )
        .context("amend spec")?;

        // get mutable root from oci spec
        let root = match spec.root_mut() {
            Some(root) => root,
            None => return Err(anyhow!("spec miss root field")),
        };

        // handler rootfs
        let rootfs = self
            .resource_manager
            .handler_rootfs(
                &config.container_id,
                root,
                &config.bundle,
                &config.rootfs_mounts,
                &updated_annotations,
            )
            .await
            .context("handler rootfs")?;

        // update rootfs
        root.set_path(
            rootfs
                .get_guest_rootfs_path()
                .await
                .context("get guest rootfs path")?
                .into(),
        );

        let mut storages = vec![];
        if let Some(mut storage_list) = rootfs.get_storage().await {
            storages.append(&mut storage_list);
        }
        inner.rootfs.push(rootfs);

        // handler volumes
        let volumes = self
            .resource_manager
            .handler_volumes(&config.container_id, &spec)
            .await
            .context("handler volumes")?;
        let mut oci_mounts = vec![];
        for v in volumes {
            let mut volume_mounts = v.get_volume_mount().context("get volume mount")?;
            if !volume_mounts.is_empty() {
                oci_mounts.append(&mut volume_mounts);
            }

            let mut s = v.get_storage().context("get storage")?;
            if !s.is_empty() {
                storages.append(&mut s);
            }
            inner.volumes.push(v);
        }
        spec.set_mounts(Some(oci_mounts));

        let linux = spec
            .linux()
            .as_ref()
            .context("OCI spec missing linux field")?;

        let container_devices = self
            .resource_manager
            .handler_devices(&config.container_id, linux)
            .await?;
        let devices_agent = annotate_container_devices(&mut spec, container_devices)
            .context("annotate container devices failed")?;

        // update vcpus, mems and host cgroups
        let resources = self
            .resource_manager
            .update_linux_resource(
                &config.container_id,
                inner.linux_resources.as_ref(),
                ResourceUpdateOp::Add,
            )
            .await?;
        if let Some(linux) = &mut spec.linux_mut() {
            linux.set_resources(resources);

            // Only CPU and Memory constraints are supported in the guest.
            // Clear unsupported resource fields to match the Go runtime
            // and satisfy the agent policy checks.
            if let Some(resource) = linux.resources_mut() {
                resource.set_devices(None);
                resource.set_pids(None);
                resource.set_block_io(None);
                resource.set_network(None);
            }

            // VFIO device filtering depends on vfio_mode configuration:
            //
            // - guest-kernel mode: Devices are managed by the guest kernel driver and
            //   are not presented to the container. Remove them from the OCI spec to
            //   match the Go runtime (kata_agent.go:1093-1105) and satisfy the agent
            //   policy's allow_linux_devices check.
            //   * vfio-pci-gk: PCI device passthrough with guest kernel driver
            //
            // - vfio mode: Devices appear as VFIO character devices
            //   (/dev/vfio/*) inside the container. Keep them in the OCI spec so the
            //   agent can validate and bind them properly. This is required for:
            //   * vfio-pci: PCI device passthrough with VFIO in container
            //   * vfio-ap: Adjunct Processor (AP) device passthrough with VFIO-AP
            let vfio_mode = toml_config.runtime.vfio_mode.as_str();
            filter_vfio_devices(linux, vfio_mode);
        }

        let container_name = k8s::container_name(&spec);
        let mut shared_mounts = Vec::new();
        for shared_mount in &toml_config.runtime.shared_mounts {
            if shared_mount.dst_ctr == container_name {
                let m = agent::types::SharedMount {
                    name: shared_mount.name.clone(),
                    src_ctr: shared_mount.src_ctr.clone(),
                    src_path: shared_mount.src_path.clone(),
                    dst_ctr: shared_mount.dst_ctr.clone(),
                    dst_path: shared_mount.dst_path.clone(),
                };
                shared_mounts.push(m);
            }
        }

        // In passfd io mode, we create vsock connections for io in advance
        // and pass port info to agent in `CreateContainerRequest`.
        // These vsock connections will be used as stdin/stdout/stderr of the container process.
        // See agent/src/passfd_io.rs for more details.
        if let Some((hvsock_uds_path, passfd_port)) = &self.passfd_listener_addr {
            inner
                .init_process
                .passfd_io_init(hvsock_uds_path, *passfd_port)
                .await?;
        }

        info!(
            sl!(),
            "OCI Spec {:?} within CreateContainerRequest.",
            spec.clone()
        );

        // create container
        let r = agent::CreateContainerRequest {
            process_id: agent::ContainerProcessID::new(&config.container_id, ""),
            storages,
            oci: Some(spec),
            sandbox_pidns,
            devices: devices_agent,
            shared_mounts,
            stdin_port: inner
                .init_process
                .passfd_io
                .as_ref()
                .and_then(|io| io.stdin_port),
            stdout_port: inner
                .init_process
                .passfd_io
                .as_ref()
                .and_then(|io| io.stdout_port),
            stderr_port: inner
                .init_process
                .passfd_io
                .as_ref()
                .and_then(|io| io.stderr_port),
            ..Default::default()
        };

        self.agent
            .create_container(r)
            .await
            .context("agent create container")?;
        self.resource_manager.dump().await;
        Ok(())
    }

    pub async fn start(
        &self,
        containers: Arc<RwLock<HashMap<String, Container>>>,
        process: &ContainerProcess,
    ) -> Result<()> {
        if matches!(process.process_type, ProcessType::Container)
            && self.config.checkpoint.is_some()
        {
            // create-with-checkpoint (`ctr c restore --live`): restore the checkpointed
            // process tree instead of starting a fresh init. Stage the CRIU images that
            // containerd extracted from the image's CRIU layer into the rootfs upperdir
            // host-side, so the guest -- and the agent's criu restore -- sees them at the
            // well-known rootfs path. (If the image instead used --rw, the rootfs already
            // carries them; this copy just refreshes from the authoritative CRIU layer.)
            let guest_cr = format!(
                "/run/kata-containers/{}/rootfs/.kata-cr",
                self.container_id.container_id
            );
            if let Some(checkpoint) = self.config.checkpoint.as_deref() {
                if !checkpoint.is_empty() && std::path::Path::new(checkpoint).exists() {
                    if let Some(upper) = self.rootfs_upperdir() {
                        let dst = format!("{}/.kata-cr", upper);
                        std::fs::create_dir_all(&dst)
                            .with_context(|| format!("create restore staging dir {}", dst))?;
                        Self::copy_dir_contents(checkpoint, &dst)
                            .context("stage CRIU images into rootfs upperdir")?;
                        info!(self.logger, "restore: staged CRIU images from containerd layer";
                            "from" => checkpoint, "to" => dst.as_str());
                    }
                }
            }
            info!(self.logger, "restoring container from checkpoint"; "image_path" => guest_cr.as_str());
            self.restore(&guest_cr)
                .await
                .context("restore container from checkpoint")?;
            let mut inner = self.inner.write().await;
            inner.set_state(ProcessStatus::Running).await;
            // Set up the exit wait so the task-exit event fires when the restored
            // (subreaped) process dies -> containerd marks the task stopped, making
            // `ctr t kill` / `ctr t rm` work on a restored container. No IO copy: the
            // restored process owns its own fds (criu restored them), so pass an empty
            // wait group.
            inner
                .init_process
                .run_io_wait(containers, self.agent.clone(), awaitgroup::WaitGroup::new())
                .await
                .context("set up restore exit wait")?;
            return Ok(());
        }
        let mut inner = self.inner.write().await;
        match process.process_type {
            ProcessType::Container => {
                if let Err(err) = inner.start_container(&process.container_id).await {
                    let device_manager = self.resource_manager.get_device_manager().await;
                    let _ = inner.stop_process(process, true, &device_manager).await;
                    return Err(err);
                }

                if self.passfd_listener_addr.is_some() {
                    inner
                        .init_process
                        .passfd_io_wait(containers, self.agent.clone())
                        .await?;
                } else {
                    let container_io = inner.new_container_io(process).await?;
                    inner
                        .init_process
                        .start_io_and_wait(containers, self.agent.clone(), container_io)
                        .await?;
                }
            }
            ProcessType::Exec => {
                // In passfd io mode, we create vsock connections for io in advance
                // and pass port info to agent in `ExecProcessRequest`.
                // These vsock connections will be used as stdin/stdout/stderr of the exec process.
                // See agent/src/passfd_io.rs for more details.
                if let Some((hvsock_uds_path, passfd_port)) = &self.passfd_listener_addr {
                    let exec = inner
                        .exec_processes
                        .get_mut(&process.exec_id)
                        .ok_or_else(|| Error::ProcessNotFound(process.clone()))?;
                    exec.process
                        .passfd_io_init(hvsock_uds_path, *passfd_port)
                        .await?;
                }

                if let Err(e) = inner.start_exec_process(process).await {
                    let device_manager = self.resource_manager.get_device_manager().await;
                    let _ = inner.stop_process(process, true, &device_manager).await;
                    return Err(e).context("enter process");
                }

                {
                    let exec = inner
                        .exec_processes
                        .get(&process.exec_id)
                        .ok_or_else(|| Error::ProcessNotFound(process.clone()))?;
                    if exec.process.height != 0 && exec.process.width != 0 {
                        inner
                            .win_resize_process(process, exec.process.height, exec.process.width)
                            .await
                            .context("win resize")?;
                    }
                }

                if self.passfd_listener_addr.is_some() {
                    // In passfd io mode, we don't bother with the IO.
                    // We send `WaitProcessRequest` immediately to the agent
                    // and wait for the response in a separate thread.
                    // The agent will only respond after IO is done.
                    let exec = inner
                        .exec_processes
                        .get_mut(&process.exec_id)
                        .ok_or_else(|| Error::ProcessNotFound(process.clone()))?;
                    exec.process
                        .passfd_io_wait(containers, self.agent.clone())
                        .await?;
                } else {
                    // In legacy io mode, we handle IO by polling the agent.
                    // When IO is done, we send `WaitProcessRequest` to agent
                    // to get the exit status.
                    let container_io =
                        inner.new_container_io(process).await.context("io stream")?;

                    let exec = inner
                        .exec_processes
                        .get_mut(&process.exec_id)
                        .ok_or_else(|| Error::ProcessNotFound(process.clone()))?;
                    exec.process
                        .start_io_and_wait(containers, self.agent.clone(), container_io)
                        .await
                        .context("start io and wait")?;
                }
            }
        }

        Ok(())
    }

    pub async fn delete_exec_process(&self, container_process: &ContainerProcess) -> Result<()> {
        let mut inner = self.inner.write().await;
        inner
            .delete_exec_process(&container_process.exec_id)
            .await
            .context("delete process")
    }

    pub async fn state_process(
        &self,
        container_process: &ContainerProcess,
    ) -> Result<ProcessStateInfo> {
        let inner = self.inner.read().await;
        match container_process.process_type {
            ProcessType::Container => inner.init_process.state().await,
            ProcessType::Exec => {
                let exec = inner
                    .exec_processes
                    .get(&container_process.exec_id)
                    .ok_or_else(|| Error::ProcessNotFound(container_process.clone()))?;
                exec.process.state().await
            }
        }
    }

    pub async fn wait_process(
        &self,
        container_process: &ContainerProcess,
    ) -> Result<ProcessWatcher> {
        let logger = logger_with_process(container_process);
        info!(logger, "start wait process");

        let inner = self.inner.read().await;
        inner
            .fetch_exit_watcher(container_process)
            .context("fetch exit watcher")
    }

    pub async fn kill_process(
        &self,
        container_process: &ContainerProcess,
        signal: u32,
        all: bool,
    ) -> Result<()> {
        let mut inner = self.inner.write().await;

        // Check if process is already stopped before signaling.
        // For SIGKILL/SIGTERM, if the process is already stopped, return success immediately.
        // This is critical for proper cleanup when VM dies - the wait thread sets status to
        // Stopped even on error, so subsequent Kill() calls will see it as already stopped.
        let is_term_signal = is_termination_signal(signal);
        let process_status = if container_process.exec_id.is_empty() {
            inner.init_process.get_status().await
        } else if let Some(exec) = inner.exec_processes.get(&container_process.exec_id) {
            exec.process.get_status().await
        } else {
            ProcessStatus::Unknown
        };

        if is_term_signal && process_status == ProcessStatus::Stopped {
            info!(
                self.logger,
                "process has already stopped, skipping signal";
                "container" => &self.container_id.container_id,
                "process" => ?container_process,
                "signal" => signal
            );
            return Ok(());
        }

        inner.signal_process(container_process, signal, all).await
    }

    pub async fn exec_process(
        &self,
        container_process: &ContainerProcess,
        stdin: Option<String>,
        stdout: Option<String>,
        stderr: Option<String>,
        terminal: bool,
        mut oci_process: OCIProcess,
    ) -> Result<()> {
        let toml_config = self.resource_manager.config().await;
        if get_disable_guest_selinux(&toml_config) {
            oci_process.set_selinux_label(None);
        }

        let process = Process::new(
            container_process,
            self.pid,
            &self.config.bundle,
            stdin,
            stdout,
            stderr,
            terminal,
        );
        let exec = Exec {
            process,
            oci_process,
        };
        let mut inner = self.inner.write().await;
        inner.add_exec_process(&container_process.exec_id, exec);
        Ok(())
    }

    pub async fn close_io(&self, container_process: &ContainerProcess) -> Result<()> {
        let mut inner = self.inner.write().await;
        inner.close_io(container_process).await
    }

    pub async fn stop_process(&self, container_process: &ContainerProcess) -> Result<()> {
        if container_process.process_type == ProcessType::Container {
            self.copy_termination_log().await;
        }

        let mut inner = self.inner.write().await;
        let device_manager = self.resource_manager.get_device_manager().await;
        inner
            .stop_process(container_process, true, &device_manager)
            .await
            .context("stop process")?;

        // update vcpus, mems and host cgroups
        if container_process.process_type == ProcessType::Container {
            self.resource_manager
                .update_linux_resource(
                    &self.config.container_id,
                    inner.linux_resources.as_ref(),
                    ResourceUpdateOp::Del,
                )
                .await?;
        }

        Ok(())
    }

    async fn copy_termination_log(&self) {
        let toml_config = self.resource_manager.config().await;
        let shared_fs = toml_config
            .hypervisor
            .get(&toml_config.runtime.hypervisor_name)
            .and_then(|h| h.shared_fs.shared_fs.as_deref());

        // When a shared filesystem is configured the host can read the
        // termination log directly.  shared_fs == None means no shared
        // filesystem (the "none" config value is normalised to None by
        // SharedFsInfo::adjust_config).
        if shared_fs.is_some() {
            return;
        }

        let annotations = self.spec.annotations().clone().unwrap_or_default();
        let policy = annotations.get("io.kubernetes.container.terminationMessagePolicy");
        if policy.map(|p| p.as_str()) != Some("File") {
            return;
        }

        let termination_path =
            match annotations.get("io.kubernetes.container.terminationMessagePath") {
                Some(p) if !p.is_empty() => p.clone(),
                _ => return,
            };

        let req = agent::GetDiagnosticDataRequest {
            log_type: "termination_log".to_string(),
            container_id: self.container_id.container_id.clone(),
        };

        // The kubelet bind-mounts a host file into the container at
        // terminationMessagePath, then reads back from that host file.
        // With shared_fs=none the guest cannot write through that mount,
        // so we locate the host-side source path from the OCI mounts and
        // write the data there directly.
        let host_path = self.spec.mounts().as_ref().and_then(|mounts| {
            mounts
                .iter()
                .find(|m| m.destination() == std::path::Path::new(&termination_path))
                .and_then(|m| m.source().clone())
        });

        let host_path = match host_path {
            Some(p) => p,
            None => {
                warn!(
                    self.logger,
                    "No host mount found for termination message path"
                );
                return;
            }
        };

        match self.agent.get_diagnostic_data(req).await {
            Ok(resp) if !resp.data.is_empty() => {
                if let Err(e) = tokio::fs::write(&host_path, resp.data.as_bytes()).await {
                    warn!(self.logger, "Failed to write termination message: {}", e);
                }
            }
            Ok(_) => {}
            Err(e) => {
                warn!(
                    self.logger,
                    "Failed to get termination message from guest: {}", e
                );
            }
        }
    }

    pub async fn pause(&self) -> Result<()> {
        let mut inner = self.inner.write().await;
        let status = inner.init_process.get_status().await;
        if status != ProcessStatus::Running {
            warn!(
                self.logger,
                "container is in {:?} state, will not pause", status
            );
            return Ok(());
        }

        self.agent
            .pause_container(self.container_id.clone().into())
            .await
            .context("agent pause container")?;
        inner.set_state(ProcessStatus::Paused).await;

        Ok(())
    }

    pub async fn resume(&self) -> Result<()> {
        let mut inner = self.inner.write().await;
        let status = inner.init_process.get_status().await;
        if status != ProcessStatus::Paused {
            warn!(
                self.logger,
                "container is in {:?} state, will not resume", status
            );
            return Ok(());
        }

        self.agent
            .resume_container(self.container_id.clone().into())
            .await
            .context("agent pause container")?;
        inner.set_state(ProcessStatus::Running).await;

        Ok(())
    }

    /// Host path of the overlay upperdir backing this container's rootfs (the guest
    /// rootfs is virtiofs-shared from it), parsed from the snapshot mount options. Used to
    /// move CRIU images between the guest rootfs and containerd's CRIU-layer path host-side.
    fn rootfs_upperdir(&self) -> Option<String> {
        self.config.rootfs_mounts.iter().find_map(|m| {
            m.options
                .iter()
                .find_map(|o| o.strip_prefix("upperdir=").map(|s| s.to_string()))
        })
    }

    /// Copy the *contents* of `src` into `dst` host-side, preserving attributes.
    fn copy_dir_contents(src: &str, dst: &str) -> Result<()> {
        let status = std::process::Command::new("cp")
            .arg("-a")
            .arg(format!("{}/.", src))
            .arg(format!("{}/", dst))
            .status()
            .context("spawn cp -a")?;
        if !status.success() {
            return Err(anyhow!(
                "cp -a {}/. {}/ failed (rc={:?})",
                src,
                dst,
                status.code()
            ));
        }
        Ok(())
    }

    pub async fn checkpoint(&self, checkpoint_path: &str) -> Result<()> {
        // The agent dumps CRIU images into the container rootfs (.kata-cr); the rootfs is
        // virtiofs-backed by the host overlay upperdir, so they appear host-side there. The
        // container is left running (CRI CheckpointContainer semantics).
        let cid = self.container_id.container_id.clone();
        let guest_cr = format!("/run/kata-containers/{}/rootfs/.kata-cr", cid);
        self.agent
            .checkpoint_container(agent::CheckpointContainerRequest {
                container_id: cid,
                image_path: guest_cr,
            })
            .await
            .context("agent checkpoint container")?;

        // Copy the images from the host upperdir into containerd's checkpoint path so they
        // land in containerd's CRIU layer. containerd diffs the --rw snapshot *before* the
        // task checkpoint runs, so the rw layer cannot carry a fresh dump; the CRIU layer
        // (this path) is the reliable channel for `ctr c checkpoint` / `ctr c restore`.
        // Stage the dumped images where the engine expects them. `ctr c checkpoint --task`
        // passes the criu-layer dir in `checkpoint_path`; containerd's CRI plugin
        // (`crictl checkpoint`) passes an empty path and instead reads from its per-container
        // state dir (.../io.containerd.grpc.v1.cri/containers/<cid>/, where it expects
        // stats-dump + the criu images). Cover both targets.
        match self.rootfs_upperdir() {
            Some(upper) => {
                let src = format!("{}/.kata-cr", upper);
                if std::path::Path::new(&src).exists() {
                    let cri_dir = format!(
                        "/var/lib/containerd/io.containerd.grpc.v1.cri/containers/{}",
                        self.container_id.container_id
                    );
                    let mut targets: Vec<String> = Vec::new();
                    if !checkpoint_path.is_empty() {
                        targets.push(checkpoint_path.to_string());
                    }
                    if std::path::Path::new(&cri_dir).is_dir() {
                        targets.push(cri_dir);
                    }
                    for t in &targets {
                        std::fs::create_dir_all(t)
                            .with_context(|| format!("create checkpoint dir {}", t))?;
                        Self::copy_dir_contents(&src, t)
                            .with_context(|| format!("copy CRIU images to {}", t))?;
                        info!(self.logger, "checkpoint: staged CRIU images";
                            "from" => src.as_str(), "to" => t.as_str());
                    }
                }
            }
            None => warn!(
                self.logger,
                "checkpoint: no rootfs upperdir found; CRIU images remain only in the rootfs"
            ),
        }
        Ok(())
    }

    pub async fn restore(&self, image_path: &str) -> Result<()> {
        // CRIU restores the checkpointed process tree in the guest from the image set.
        self.agent
            .restore_container(agent::RestoreContainerRequest {
                container_id: self.container_id.container_id.clone(),
                image_path: image_path.to_owned(),
            })
            .await
            .context("agent restore container")?;

        Ok(())
    }

    pub async fn resize_pty(
        &self,
        process: &ContainerProcess,
        width: u32,
        height: u32,
    ) -> Result<()> {
        let logger = logger_with_process(process);
        let mut inner = self.inner.write().await;
        if inner.init_process.get_status().await != ProcessStatus::Running {
            warn!(logger, "container is not running");
            return Ok(());
        }

        if process.exec_id.is_empty() {
            inner.init_process.height = height;
            inner.init_process.width = width;
        } else if let Some(exec) = inner.exec_processes.get_mut(&process.exec_id) {
            exec.process.height = height;
            exec.process.width = width;

            // for some case, resize_pty request should be handled while the process has not been started in agent
            // just return here, and truly resize_pty will happen in start_process
            if exec.process.get_status().await != ProcessStatus::Running {
                return Ok(());
            }
        } else {
            return Err(anyhow!(
                "could not find process {} in container {}",
                process.exec_id(),
                process.container_id()
            ));
        }

        inner.win_resize_process(process, height, width).await
    }

    pub async fn stats(&self) -> Result<Option<agent::StatsContainerResponse>> {
        let stats_resp = self
            .agent
            .stats_container(self.container_id.clone().into())
            .await
            .context("agent stats container")?;
        Ok(Some(stats_resp))
    }

    pub async fn update(&self, resources: &LinuxResources) -> Result<()> {
        let mut inner = self.inner.write().await;
        inner.linux_resources = Some(resources.clone());
        // update vcpus, mems and host cgroups
        let agent_resources = self
            .resource_manager
            .update_linux_resource(
                &self.config.container_id,
                Some(resources),
                ResourceUpdateOp::Update,
            )
            .await?;

        let req = agent::UpdateContainerRequest {
            container_id: self.container_id.container_id.clone(),
            resources: agent_resources,
            mounts: Vec::new(),
        };
        self.agent
            .update_container(req)
            .await
            .context("agent update container")?;
        Ok(())
    }

    pub async fn config(&self) -> ContainerConfig {
        self.config.clone()
    }

    pub async fn spec(&self) -> oci::Spec {
        self.spec.clone()
    }

    pub async fn cleanup(&mut self) -> Result<()> {
        let mut inner = self.inner.write().await;
        let device_manager = self.resource_manager.get_device_manager().await;
        inner
            .cleanup_container(
                self.container_id.container_id.as_str(),
                true,
                &device_manager,
            )
            .await
    }
}

fn amend_spec(
    spec: &mut oci::Spec,
    disable_guest_seccomp: bool,
    disable_guest_selinux: bool,
    disable_guest_empty_dir: bool,
    emptydir_mode: &str,
) -> Result<()> {
    // Only the StartContainer hook needs to be reserved for execution in the guest
    if let Some(hooks) = spec.hooks().as_ref() {
        let mut oci_hooks = oci::Hooks::default();
        oci_hooks.set_start_container(hooks.start_container().clone());
        spec.set_hooks(Some(oci_hooks));
    }

    // special process K8s ephemeral volumes.
    update_ephemeral_storage_type(spec, disable_guest_empty_dir, emptydir_mode);

    if let Some(linux) = &mut spec.linux_mut() {
        if disable_guest_seccomp {
            linux.set_seccomp(None);
        }

        // Host pidns path does not make sense in kata. Let's just align it with
        // sandbox namespace whenever it is set.
        let ns: Vec<oci::LinuxNamespace> = linux
            .namespaces()
            .clone()
            .unwrap_or_default()
            .iter()
            .filter(|n| {
                n.typ() != oci::LinuxNamespaceType::Pid
                    && n.typ() != oci::LinuxNamespaceType::Network
            })
            .map(|n| {
                let mut ns = oci::LinuxNamespace::default();
                ns.set_typ(n.typ());
                ns
            })
            .collect();

        linux.set_namespaces(if ns.is_empty() { None } else { Some(ns) });
    }

    if disable_guest_selinux {
        if let Some(ref mut process) = spec.process_mut() {
            process.set_selinux_label(None);
        }
        if let Some(ref mut linux) = spec.linux_mut() {
            linux.set_mount_label(None);
        }
    }

    Ok(())
}

fn get_disable_guest_selinux(toml_config: &TomlConfig) -> bool {
    match toml_config
        .hypervisor
        .get(&toml_config.runtime.hypervisor_name)
    {
        Some(hypervisor_config) => hypervisor_config.disable_guest_selinux,
        // This shouldn't happen due to how logic in the config crate works
        // but we need to handle it anyway so we stick with the default
        // value of disable_guest_selinux in configuration.toml which
        // is 'true'.
        None => true,
    }
}

// is_pid_namespace_enabled checks if Pid namespace for a container needs to be shared with its sandbox
// pid namespace.
fn is_pid_namespace_enabled(spec: &oci::Spec) -> bool {
    if let Some(linux) = spec.linux().as_ref() {
        let namespaces = linux.namespaces().clone().unwrap_or_default();
        for n in namespaces.iter() {
            if n.typ() == oci::LinuxNamespaceType::Pid {
                return !n.path().is_none();
            }
        }
    }

    false
}

/// Filter VFIO devices from the Linux device list based on vfio_mode configuration.
/// - vfio mode: Keeps all devices including /dev/vfio/*
/// - guest-kernel mode: Removes /dev/vfio/* devices as they're managed by guest kernel
///   Note that the guest-kernel mode is assumed if vfio_mode is unset/empty.
fn filter_vfio_devices(linux: &mut oci::Linux, vfio_mode: &str) {
    if vfio_mode == "vfio" {
        return;
    }

    const VFIO_PATH: &str = "/dev/vfio/";
    let filtered = linux.devices().as_ref().map(|devices| {
        devices
            .iter()
            .filter(|d| {
                !(d.typ() == oci::LinuxDeviceType::C
                    && d.path().to_str().is_some_and(|p| p.starts_with(VFIO_PATH)))
            })
            .cloned()
            .collect::<Vec<_>>()
    });
    linux.set_devices(match filtered {
        Some(v) if v.is_empty() => None,
        other => other,
    });
}

#[cfg(test)]
mod tests {
    use super::amend_spec;
    use super::is_pid_namespace_enabled;
    use super::*;
    use oci_spec::runtime::LinuxNamespaceType;
    use oci_spec::runtime::{LinuxBuilder, LinuxNamespaceBuilder};

    #[test]
    fn test_amend_spec_disable_guest_seccomp() {
        let mut spec = oci::Spec::default();
        let mut linux = oci::Linux::default();
        linux.set_seccomp(Some(oci::LinuxSeccomp::default()));
        spec.set_linux(Some(linux));

        assert!(spec.linux().as_ref().unwrap().seccomp().is_some());

        // disable_guest_seccomp = false
        amend_spec(&mut spec, false, false, false, "shared-fs").unwrap();
        assert!(spec.linux().as_ref().unwrap().seccomp().is_some());

        // disable_guest_seccomp = true
        amend_spec(&mut spec, true, false, false, "shared-fs").unwrap();
        assert!(spec.linux().as_ref().unwrap().seccomp().is_none());
    }

    #[test]
    fn test_amend_spec_disable_guest_selinux() {
        let mut spec = oci::SpecBuilder::default()
            .process(
                oci::ProcessBuilder::default()
                    .selinux_label("xxx".to_owned())
                    .build()
                    .unwrap(),
            )
            .linux(
                oci::LinuxBuilder::default()
                    .mount_label("yyy".to_owned())
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap();

        // disable_guest_selinux = false, selinux labels are left alone
        amend_spec(&mut spec, false, false, false, "shared-fs").unwrap();
        assert!(spec.process().as_ref().unwrap().selinux_label() == &Some("xxx".to_owned()));
        assert!(spec.linux().as_ref().unwrap().mount_label() == &Some("yyy".to_owned()));

        // disable_guest_selinux = true, selinux labels are reset
        amend_spec(&mut spec, false, true, false, "shared-fs").unwrap();
        assert!(spec.process().as_ref().unwrap().selinux_label().is_none());
        assert!(spec.linux().as_ref().unwrap().mount_label().is_none());
    }

    #[test]
    fn test_is_pid_namespace_enabled() {
        struct TestData<'a> {
            desc: &'a str,
            namespaces: Vec<oci::LinuxNamespace>,
            result: bool,
        }

        let tests = &[
            TestData {
                desc: "no pid namespace",
                namespaces: vec![LinuxNamespaceBuilder::default()
                    .typ(LinuxNamespaceType::Network)
                    .path("/dev/null")
                    .build()
                    .unwrap()],
                result: false,
            },
            TestData {
                desc: "empty pid namespace path",
                namespaces: vec![
                    LinuxNamespaceBuilder::default()
                        .typ(LinuxNamespaceType::Network)
                        .build()
                        .unwrap(),
                    LinuxNamespaceBuilder::default()
                        .typ(LinuxNamespaceType::Pid)
                        .build()
                        .unwrap(),
                ],
                result: false,
            },
            TestData {
                desc: "pid namespace is set",
                namespaces: vec![
                    LinuxNamespaceBuilder::default()
                        .typ(LinuxNamespaceType::Network)
                        .path("/some/path")
                        .build()
                        .unwrap(),
                    LinuxNamespaceBuilder::default()
                        .typ(LinuxNamespaceType::Pid)
                        .path("/dev/null")
                        .build()
                        .unwrap(),
                ],
                result: true,
            },
        ];

        let mut spec = oci::Spec::default();

        for (i, d) in tests.iter().enumerate() {
            spec.set_linux(Some(
                LinuxBuilder::default()
                    .namespaces(d.namespaces.clone())
                    .build()
                    .unwrap(),
            ));
            // spec.linux = Some(oci::Linux {
            //     namespaces: d.namespaces.clone(),
            //     ..Default::default()
            // });

            assert_eq!(
                d.result,
                is_pid_namespace_enabled(&spec),
                "test[{}]: {:?}",
                i,
                d.desc
            );
        }
    }

    #[test]
    fn test_filter_vfio_devices_guest_kernel_mode() {
        // Test that VFIO devices are filtered out in guest-kernel mode
        let vfio_device = oci::LinuxDeviceBuilder::default()
            .path("/dev/vfio/1")
            .typ(oci::LinuxDeviceType::C)
            .major(10)
            .minor(196)
            .build()
            .unwrap();

        let non_vfio_device = oci::LinuxDeviceBuilder::default()
            .path("/dev/null")
            .typ(oci::LinuxDeviceType::C)
            .major(1)
            .minor(3)
            .build()
            .unwrap();

        let mut linux = oci::LinuxBuilder::default()
            .devices(vec![vfio_device, non_vfio_device.clone()])
            .build()
            .unwrap();

        filter_vfio_devices(&mut linux, "guest-kernel");

        let devices = linux.devices().as_ref().unwrap();
        assert_eq!(devices.len(), 1, "Should have only 1 device after filtering");
        assert_eq!(
            devices[0].path(),
            non_vfio_device.path(),
            "Non-VFIO device should be preserved"
        );
    }

    #[test]
    fn test_filter_vfio_devices_vfio_mode() {
        // Test that VFIO devices are preserved in vfio mode
        let vfio_device = oci::LinuxDeviceBuilder::default()
            .path("/dev/vfio/1")
            .typ(oci::LinuxDeviceType::C)
            .major(10)
            .minor(196)
            .build()
            .unwrap();

        let non_vfio_device = oci::LinuxDeviceBuilder::default()
            .path("/dev/null")
            .typ(oci::LinuxDeviceType::C)
            .major(1)
            .minor(3)
            .build()
            .unwrap();

        let mut linux = oci::LinuxBuilder::default()
            .devices(vec![vfio_device, non_vfio_device])
            .build()
            .unwrap();

        filter_vfio_devices(&mut linux, "vfio");

        let devices = linux.devices().as_ref().unwrap();
        assert_eq!(devices.len(), 2, "Should have both devices in vfio mode");
    }

    #[test]
    fn test_filter_vfio_devices_only_vfio_filtered() {
        // Test that only /dev/vfio/* devices are filtered in guest-kernel mode
        let vfio_device = oci::LinuxDeviceBuilder::default()
            .path("/dev/vfio/1")
            .typ(oci::LinuxDeviceType::C)
            .major(10)
            .minor(196)
            .build()
            .unwrap();

        let vfio_container = oci::LinuxDeviceBuilder::default()
            .path("/dev/vfio/vfio")
            .typ(oci::LinuxDeviceType::C)
            .major(10)
            .minor(196)
            .build()
            .unwrap();

        let similar_path = oci::LinuxDeviceBuilder::default()
            .path("/dev/vfio-test")
            .typ(oci::LinuxDeviceType::C)
            .major(1)
            .minor(1)
            .build()
            .unwrap();

        let mut linux = oci::LinuxBuilder::default()
            .devices(vec![
                vfio_device,
                vfio_container,
                similar_path.clone(),
            ])
            .build()
            .unwrap();

        filter_vfio_devices(&mut linux, "guest-kernel");

        let devices = linux.devices().as_ref().unwrap();
        assert_eq!(
            devices.len(),
            1,
            "Should only filter devices starting with /dev/vfio/"
        );
        assert_eq!(
            devices[0].path(),
            similar_path.path(),
            "Device with similar but different path should be preserved"
        );
    }

    #[test]
    fn test_filter_vfio_devices_empty_mode() {
        // Test default/empty mode behavior (should filter /dev/vfio/* like guest-kernel mode)
        let vfio_device = oci::LinuxDeviceBuilder::default()
            .path("/dev/vfio/1")
            .typ(oci::LinuxDeviceType::C)
            .major(10)
            .minor(196)
            .build()
            .unwrap();

        let mut linux = oci::LinuxBuilder::default()
            .devices(vec![vfio_device])
            .build()
            .unwrap();

        filter_vfio_devices(&mut linux, "");

        assert!(linux.devices().is_none(), "Should filter out VFIO device with empty mode");
    }

    #[test]
    fn test_filter_vfio_devices_no_devices() {
        // Test that filtering works when there are no devices
        let mut linux = oci::LinuxBuilder::default().build().unwrap();

        filter_vfio_devices(&mut linux, "guest-kernel");

        assert!(linux.devices().is_none(), "Should remain None when no devices");
    }
}
