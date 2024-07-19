// From redhook, https://github.com/geofft/redhook, modified to not crash on non-existent interposed.

use libc::{c_char, c_void};

#[link(name = "dl")]
extern "C" {
    fn dlsym(handle: *const c_void, symbol: *const c_char) -> *const c_void;
}

const RTLD_NEXT: *const c_void = -1isize as *const c_void;

// NOTE: May be the null pointer.
pub unsafe fn dlsym_next(symbol: &'static str) -> *const u8 {
    let ptr = dlsym(RTLD_NEXT, symbol.as_ptr() as *const c_char);
    ptr as *const u8
}

#[macro_export]
macro_rules! hook {
    (unsafe fn $real_fn:ident ( $($v:ident : $t:ty),* ) -> $r:ty => $hook_fn:ident $body:block) => {
        #[allow(non_camel_case_types)]
        pub struct $real_fn {__private_field: ()}
        #[allow(non_upper_case_globals)]
        static $real_fn: $real_fn = $real_fn {__private_field: ()};

        impl $real_fn {
            fn get(&self) -> unsafe extern fn ( $($v : $t),* ) -> $r {
                use ::std::sync::Once;

                static mut REAL: *const u8 = 0 as *const u8;
                static mut ONCE: Once = Once::new();

                unsafe {
                    ONCE.call_once(|| {
                        REAL = $crate::interposing_linux::dlsym_next(concat!(stringify!($real_fn), "\0"));
                        if REAL == null() {
                            panic!("XetLDFS: Attempting to call hook to non-existant function {}.", stringify!($real_fn));
                        }
                    });
                    ::std::mem::transmute(REAL)
                }
            }

            #[no_mangle]
            pub unsafe extern fn $real_fn ( $($v : $t),* ) -> $r {
                ::std::panic::catch_unwind(|| $hook_fn ( $($v),* )).unwrap_or_else(|_| $real_fn.get() ( $($v),* ))
            }
        }

        pub unsafe fn $hook_fn ( $($v : $t),* ) -> $r {
            $body
        }
    };

    (unsafe fn $real_fn:ident ( $($v:ident : $t:ty),* ) => $hook_fn:ident $body:block) => {
        $crate::hook! { unsafe fn $real_fn ( $($v : $t),* ) -> () => $hook_fn $body }
    };
}

#[macro_export]
macro_rules! real {
    ($real_fn:ident) => {
        let fn_ptr = $real_fn.get();
        if fn_ptr == null {
            panic!(
                "XetLDFS: AAttempting to call non-existant function {}.",
                stringify!($real_fn)
            );
        }
        fn_ptr
    };
}

#[macro_export]
macro_rules! fn_is_valid {
    ($real_fn:ident) => {
        $real_fn.get() != null()
    };
}
