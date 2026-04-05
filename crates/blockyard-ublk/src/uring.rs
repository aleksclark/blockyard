#[cfg(target_os = "linux")]
pub const UBLK_CTRL_DEV_PATH: &str = "/dev/ublk-control";

pub const UBLK_CMD_ADD_DEV: u32 = 0x04;
pub const UBLK_CMD_START_DEV: u32 = 0x05;
pub const UBLK_CMD_STOP_DEV: u32 = 0x06;
pub const UBLK_CMD_DEL_DEV: u32 = 0x07;
pub const UBLK_CMD_GET_DEV_INFO: u32 = 0x08;

pub const UBLK_IO_FETCH_REQ: u32 = 0x20;
pub const UBLK_IO_COMMIT_AND_FETCH_REQ: u32 = 0x21;

pub const UBLK_IO_OP_READ: u32 = 0;
pub const UBLK_IO_OP_WRITE: u32 = 1;
pub const UBLK_IO_OP_FLUSH: u32 = 2;
pub const UBLK_IO_OP_DISCARD: u32 = 3;

pub const UBLK_IO_RES_OK: i32 = 0;
pub const UBLK_IO_RES_ABORT: i32 = -libc::ENODEV;

pub const UBLK_F_SUPPORT_ZERO_COPY: u64 = 1 << 0;
pub const UBLK_F_URING_CMD_COMP_IN_TASK: u64 = 1 << 1;
pub const UBLK_F_NEED_GET_DATA: u64 = 1 << 2;

#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct UblkDevInfo {
    pub nr_hw_queues: u16,
    pub queue_depth: u16,
    pub state: u16,
    pub pad0: u16,
    pub max_io_buf_bytes: u32,
    pub dev_id: u32,
    pub ublksrv_pid: i32,
    pub pad1: u32,
    pub flags: u64,
    pub ublksrv_flags: u64,
    pub reserved0: u64,
    pub reserved1: u64,
    pub reserved2: u64,
}

impl Default for UblkDevInfo {
    fn default() -> Self {
        // SAFETY: all-zero is valid for this POD C struct
        unsafe { std::mem::zeroed() }
    }
}

#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct UblkCtrlCmd {
    pub dev_id: u32,
    pub queue_id: u16,
    pub len: u16,
    pub addr: u64,
    pub data: [u64; 1],
    pub dev_path_len: u16,
    pub pad: u16,
    pub reserved: u32,
}

impl Default for UblkCtrlCmd {
    fn default() -> Self {
        // SAFETY: all-zero is valid for this POD C struct
        unsafe { std::mem::zeroed() }
    }
}

#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct UblkIoCmd {
    pub q_id: u16,
    pub tag: u16,
    pub result: i32,
    pub addr: u64,
}

pub fn ublk_ctrl_ioctl(cmd_id: u32) -> u32 {
    let size = std::mem::size_of::<UblkCtrlCmd>() as u32;
    let nrbits = 8u32;
    let typebits = 8u32;
    let sizebits = 14u32;
    let nrshift = 0u32;
    let typeshift = nrshift + nrbits;
    let sizeshift = typeshift + typebits;
    let dirshift = sizeshift + sizebits;

    let dir: u32 = 3; // _IOWR
    let typ: u8 = b'u';

    (dir << dirshift) | ((typ as u32) << typeshift) | (cmd_id << nrshift) | (size << sizeshift)
}

pub fn ublk_io_ioctl(cmd_id: u32) -> u32 {
    let size = std::mem::size_of::<UblkIoCmd>() as u32;
    let nrbits = 8u32;
    let typebits = 8u32;
    let sizebits = 14u32;
    let nrshift = 0u32;
    let typeshift = nrshift + nrbits;
    let sizeshift = typeshift + typebits;
    let dirshift = sizeshift + sizebits;

    let dir: u32 = 3; // _IOWR
    let typ: u8 = b'u';

    (dir << dirshift) | ((typ as u32) << typeshift) | (cmd_id << nrshift) | (size << sizeshift)
}

pub fn ublk_dev_path(dev_id: u32) -> String {
    format!("/dev/ublkb{dev_id}")
}

pub fn ublk_char_path(dev_id: u32) -> String {
    format!("/dev/ublkc{dev_id}")
}

#[derive(Debug)]
pub struct UblkQueueConfig {
    pub queue_id: u16,
    pub queue_depth: u16,
    pub io_buf_size: u32,
}

#[derive(Debug)]
pub struct UblkDevConfig {
    pub dev_id: u32,
    pub nr_hw_queues: u16,
    pub queue_depth: u16,
    pub max_io_buf_bytes: u32,
    pub dev_size: u64,
    pub flags: u64,
}

impl Default for UblkDevConfig {
    fn default() -> Self {
        Self {
            dev_id: 0,
            nr_hw_queues: 1,
            queue_depth: 128,
            max_io_buf_bytes: 512 * 1024,
            dev_size: 0,
            flags: UBLK_F_URING_CMD_COMP_IN_TASK,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ublk_cmd_constants() {
        assert_eq!(UBLK_CMD_ADD_DEV, 0x04);
        assert_eq!(UBLK_CMD_START_DEV, 0x05);
        assert_eq!(UBLK_CMD_STOP_DEV, 0x06);
        assert_eq!(UBLK_CMD_DEL_DEV, 0x07);
        assert_eq!(UBLK_CMD_GET_DEV_INFO, 0x08);
    }

    #[test]
    fn test_ublk_io_constants() {
        assert_eq!(UBLK_IO_FETCH_REQ, 0x20);
        assert_eq!(UBLK_IO_COMMIT_AND_FETCH_REQ, 0x21);
    }

    #[test]
    fn test_ublk_io_op_constants() {
        assert_eq!(UBLK_IO_OP_READ, 0);
        assert_eq!(UBLK_IO_OP_WRITE, 1);
        assert_eq!(UBLK_IO_OP_FLUSH, 2);
        assert_eq!(UBLK_IO_OP_DISCARD, 3);
    }

    #[test]
    fn test_ublk_dev_info_size() {
        assert_eq!(std::mem::size_of::<UblkDevInfo>(), 64);
    }

    #[test]
    fn test_ublk_ctrl_cmd_size() {
        assert_eq!(std::mem::size_of::<UblkCtrlCmd>(), 32);
    }

    #[test]
    fn test_ublk_io_cmd_size() {
        assert_eq!(std::mem::size_of::<UblkIoCmd>(), 16);
    }

    #[test]
    fn test_ublk_dev_info_default() {
        let info = UblkDevInfo::default();
        assert_eq!(info.dev_id, 0);
        assert_eq!(info.nr_hw_queues, 0);
        assert_eq!(info.queue_depth, 0);
    }

    #[test]
    fn test_ublk_ctrl_cmd_default() {
        let cmd = UblkCtrlCmd::default();
        assert_eq!(cmd.dev_id, 0);
        assert_eq!(cmd.addr, 0);
    }

    #[test]
    fn test_ublk_dev_path() {
        assert_eq!(ublk_dev_path(0), "/dev/ublkb0");
        assert_eq!(ublk_dev_path(5), "/dev/ublkb5");
    }

    #[test]
    fn test_ublk_char_path() {
        assert_eq!(ublk_char_path(0), "/dev/ublkc0");
        assert_eq!(ublk_char_path(3), "/dev/ublkc3");
    }

    #[test]
    fn test_ctrl_ioctl_encoding() {
        let ioctl = ublk_ctrl_ioctl(UBLK_CMD_ADD_DEV);
        assert_ne!(ioctl, 0);
        let dir = ioctl >> 30;
        assert_eq!(dir, 3); // _IOWR
    }

    #[test]
    fn test_io_ioctl_encoding() {
        let ioctl = ublk_io_ioctl(UBLK_IO_FETCH_REQ);
        assert_ne!(ioctl, 0);
        let dir = ioctl >> 30;
        assert_eq!(dir, 3); // _IOWR
    }

    #[test]
    fn test_ctrl_ioctl_different_per_cmd() {
        let add = ublk_ctrl_ioctl(UBLK_CMD_ADD_DEV);
        let start = ublk_ctrl_ioctl(UBLK_CMD_START_DEV);
        let stop = ublk_ctrl_ioctl(UBLK_CMD_STOP_DEV);
        assert_ne!(add, start);
        assert_ne!(start, stop);
    }

    #[test]
    fn test_feature_flags() {
        assert_eq!(UBLK_F_SUPPORT_ZERO_COPY, 1);
        assert_eq!(UBLK_F_URING_CMD_COMP_IN_TASK, 2);
        assert_eq!(UBLK_F_NEED_GET_DATA, 4);
    }

    #[test]
    fn test_ublk_dev_config_default() {
        let cfg = UblkDevConfig::default();
        assert_eq!(cfg.dev_id, 0);
        assert_eq!(cfg.nr_hw_queues, 1);
        assert_eq!(cfg.queue_depth, 128);
        assert_eq!(cfg.max_io_buf_bytes, 512 * 1024);
        assert_eq!(cfg.flags, UBLK_F_URING_CMD_COMP_IN_TASK);
    }

    #[test]
    fn test_ublk_res_constants() {
        assert_eq!(UBLK_IO_RES_OK, 0);
        assert!(UBLK_IO_RES_ABORT < 0);
    }

    #[test]
    fn test_queue_config() {
        let qcfg = UblkQueueConfig {
            queue_id: 0,
            queue_depth: 128,
            io_buf_size: 512 * 1024,
        };
        assert_eq!(qcfg.queue_id, 0);
        assert_eq!(qcfg.queue_depth, 128);
    }

    #[test]
    #[ignore] // requires root + ublk_drv module loaded
    fn test_ublk_control_device_exists() {
        assert!(
            std::path::Path::new("/dev/ublk-control").exists(),
            "ublk_drv module not loaded"
        );
    }
}
