pub struct LinearProbing {
    pos: usize,
    mask: usize,
}

impl LinearProbing {
    #[inline]
    pub fn compute(hash: crate::HashType, mask: usize) -> LinearProbing {
        LinearProbing {
            pos: hash as usize & mask,
            mask,
        }
    }

    #[inline]
    pub fn next_probe(&mut self) -> usize {
        // Not saving the masked version removes a dependency.
        let pos = self.pos;
        self.pos = (pos + 1) & self.mask;
        pos
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_linear_probing() {
        let mut lp = LinearProbing::compute(3, 8 - 1);
        assert_eq!(lp.next_probe(), 3);
        assert_eq!(lp.next_probe(), 4);
        assert_eq!(lp.next_probe(), 5);
        assert_eq!(lp.next_probe(), 6);
        assert_eq!(lp.next_probe(), 7);
        assert_eq!(lp.next_probe(), 0);
        assert_eq!(lp.next_probe(), 1);
        assert_eq!(lp.next_probe(), 2);
    }
}
