#[cfg(all(loom, test))]
mod concurrency_types {
    pub use loom::sync;
    pub use loom::cell;
}

#[cfg(not(all(loom, test)))]
mod concurrency_types {
    pub use std::sync;

    pub mod cell {
        #[derive(Default, Debug)]
        pub(crate) struct UnsafeCell<T>(std::cell::UnsafeCell<T>);

        impl<T> UnsafeCell<T> {
            pub(crate) fn new(data: T) -> UnsafeCell<T> {
                UnsafeCell(std::cell::UnsafeCell::new(data))
            }

            pub(crate) fn with<R>(&self, f: impl FnOnce(*const T) -> R) -> R {
                f(self.0.get())
            }

            pub(crate) fn with_mut<R>(&self, f: impl FnOnce(*mut T) -> R) -> R {
                f(self.0.get())
            }
        }
    }
}

pub use concurrency_types::*;
