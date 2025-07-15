#[cfg(all(loom, test))]
mod loom_or_not {
    pub use loom::sync;

    pub fn arc_from_box<T: ?Sized>(boxed: Box<T>) -> sync::Arc<T> {
        sync::Arc::from_std(std::sync::Arc::from(boxed))
    }
}

#[cfg(not(all(loom, test)))]
mod loom_or_not {
    pub use std::sync;

    pub fn arc_from_box<T: ?Sized>(boxed: Box<T>) -> sync::Arc<T> {
        sync::Arc::from(boxed)
    }
}

pub use loom_or_not::*;
