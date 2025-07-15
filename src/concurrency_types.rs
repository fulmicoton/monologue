#[cfg(all(loom, test))]
mod concurrency_types {
    pub use loom::sync;
}

#[cfg(not(all(loom, test)))]
mod concurrency_types {
    pub use std::sync;
}

pub use concurrency_types::*;
