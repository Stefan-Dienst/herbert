use log::{info, warn, error};


fn main() {
    env_logger::init();
    info!("test")
}

fn add(x: i32, y: i32) -> i32 {
    x + y
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add() {
        assert_eq!(add(1, 2), 1 + 2)
    }

}
