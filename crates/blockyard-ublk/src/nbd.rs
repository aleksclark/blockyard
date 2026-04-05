pub struct NbdFallback;

impl NbdFallback {
    pub fn new() -> Self {
        Self
    }
}

impl Default for NbdFallback {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nbd_fallback_new() {
        let _nbd = NbdFallback::new();
    }

    #[test]
    fn test_nbd_fallback_default() {
        let _nbd = NbdFallback::default();
    }
}
