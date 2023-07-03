use lazy_static::lazy_static;
use prometheus::{register_int_counter, register_int_gauge, IntCounter, IntGauge};

lazy_static! {
    pub static ref MOUNT_POINTER_BYTES_READ: IntCounter = register_int_counter!(
        "mount_pointer_bytes_read",
        "Number of bytes read that originate from a pointer file",
    )
    .unwrap();
    pub static ref MOUNT_PASSTHROUGH_BYTES_READ: IntCounter = register_int_counter!(
        "mount_passthrough_bytes_read",
        "Number of bytes read that originate from a passthrough file",
    )
    .unwrap();
    pub static ref MOUNT_NUM_OBJECTS: IntGauge =
        register_int_gauge!("mount_num_fs_objects", "Number of filesystem objects").unwrap();
}
