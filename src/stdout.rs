#[cfg(test)]
mod tests {
    use ::core::ptr;

    #[link(name = "i13c", kind = "static")]
    extern "C" {
        fn stdout_print(len: usize, txt: *const u8) -> i64;
        fn stdout_printf(fmt: *const u8, arg1: *const u8) -> i64;
    }

    #[test]
    fn can_print_text() {
        let txt = "-- hello, world!\n";
        let len = txt.len();

        unsafe {
            assert_eq!(0, stdout_print(len, txt.as_ptr()));
        }
    }

    #[test]
    fn can_printf_without_substitution() {
        let fmt = b"-- hello, nothing!\n\0";
        let arg = ptr::null();

        unsafe {
            assert_eq!(0, stdout_printf(fmt.as_ptr(), arg));
        }
    }

    #[test]
    fn can_printf_with_single_substitution() {
        let fmt = b"-- hello, %s!\n\0";
        let arg = b"i13c\0".as_ptr();

        unsafe {
            assert_eq!(0, stdout_printf(fmt.as_ptr(), arg));
        }
    }
}
