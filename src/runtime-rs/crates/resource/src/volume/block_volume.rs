// Copyright (c) 2019-2022 Alibaba Cloud
// Copyright (c) 2019-2022 Ant Group
//
// SPDX-License-Identifier: Apache-2.0
//

use anyhow::{Context, Result};
use async_trait::async_trait;
use nix::sys::{stat, stat::SFlag};
use tokio::sync::RwLock;

use super::Volume;
use crate::volume::utils::{handle_block_volume, DEFAULT_VOLUME_FS_TYPE, KATA_MOUNT_BIND_TYPE};
use hypervisor::{
    device::{
        device_manager::{do_handle_device, get_block_driver, DeviceManager},
        DeviceConfig,
    },
    BlockConfig,
};

#[derive(Clone)]
pub(crate) struct BlockVolume {
    storage: Option<agent::Storage>,
    mount: oci::Mount,
    device_id: String,
}

/// BlockVolume for bind-mount block volume
impl BlockVolume {
    pub(crate) async fn new(
        d: &RwLock<DeviceManager>,
        m: &oci::Mount,
        read_only: bool,
        sid: &str,
    ) -> Result<Self> {
        let mnt_src: &str = &m.source;
        let block_driver = get_block_driver(d).await;
        let fstat = stat::stat(mnt_src).context(format!("stat {}", m.source))?;
        let block_device_config = BlockConfig {
            major: stat::major(fstat.st_rdev) as i64,
            minor: stat::minor(fstat.st_rdev) as i64,
            driver_option: block_driver,
            ..Default::default()
        };

        // create and insert block device into Kata VM
        let device_info = do_handle_device(d, &DeviceConfig::BlockCfg(block_device_config.clone()))
            .await
            .context("do handle device failed.")?;

<<<<<<< HEAD
        let block_volume =
            handle_block_volume(device_info, m, read_only, sid, DEFAULT_VOLUME_FS_TYPE)
                .await
                .context("do handle block volume failed")?;
=======
        // storage
        let mut storage = agent::Storage {
            options: if read_only {
                vec!["ro".to_string()]
            } else {
                Vec::new()
            },
            ..Default::default()
        };

        // As the true Block Device wrapped in DeviceType, we need to
        // get it out from the wrapper, and the device_id will be for
        // BlockVolume.
        // safe here, device_info is correct and only unwrap it.
        let mut device_id = String::new();
        if let DeviceType::Block(device) = device_info {
            // blk, mmioblk
            storage.driver = device.config.driver_option;
            // /dev/vdX
            storage.source = device.config.virt_path;
            device_id = device.device_id;
        }

        // generate host guest shared path
        let guest_path = generate_shared_path(m.destination.clone(), read_only, &device_id, sid)
            .await
            .context("generate host-guest shared path failed")?;
        storage.mount_point = guest_path.clone();

        // In some case, dest is device /dev/xxx
        if m.destination.clone().starts_with("/dev") {
            storage.fs_type = "bind".to_string();
            storage.options.append(&mut m.options.clone());
        } else {
            // usually, the dest is directory.
            storage.fs_type = blk_dev_fstype;
        }

        let mount = oci::Mount {
            destination: m.destination.clone(),
            r#type: storage.fs_type.clone(),
            source: guest_path,
            options: m.options.clone(),
            uid_mappings: m.uid_mappings.clone(),
            gid_mappings: m.gid_mappings.clone(),
        };
>>>>>>> 70152c24e (tmp commit)

        Ok(Self {
            storage: Some(block_volume.0),
            mount: block_volume.1,
            device_id: block_volume.2,
        })
    }
}

#[async_trait]
impl Volume for BlockVolume {
    fn get_volume_mount(&self) -> Result<Vec<oci::Mount>> {
        Ok(vec![self.mount.clone()])
    }

    fn get_storage(&self) -> Result<Vec<agent::Storage>> {
        let s = if let Some(s) = self.storage.as_ref() {
            vec![s.clone()]
        } else {
            vec![]
        };

        Ok(s)
    }

    async fn cleanup(&self, device_manager: &RwLock<DeviceManager>) -> Result<()> {
        device_manager
            .write()
            .await
            .try_remove_device(&self.device_id)
            .await
    }

    fn get_device_id(&self) -> Result<Option<String>> {
        Ok(Some(self.device_id.clone()))
    }
}

pub(crate) fn is_block_volume(m: &oci::Mount) -> bool {
    if m.r#type.as_str() != KATA_MOUNT_BIND_TYPE {
        return false;
    }

    match stat::stat(m.source.as_str()) {
        Ok(fstat) => SFlag::from_bits_truncate(fstat.st_mode) == SFlag::S_IFBLK,
        Err(_) => false,
    }
}
