#[cfg(test)]
mod tests {
    #[repr(C)]
    struct CoopInfo {
        data: [u64; 33],
    }

    impl Default for CoopInfo {
        fn default() -> Self {
            CoopInfo { data: [0; 33] }
        }
    }

    #[repr(C)]
    struct CoopContext {
        ptr: *const u64,
    }

    impl CoopContext {
        fn new(ptr: *const u64) -> Self {
            CoopContext { ptr }
        }

        fn get(&self) -> u64 {
            unsafe { std::ptr::read_volatile(self.ptr) }
        }

        fn add(&self, val: u64) {
            unsafe {
                std::ptr::write_volatile(
                    self.ptr as *mut u64,
                    std::ptr::read_volatile(self.ptr) + val,
                );
            }
        }
    }

    type CoopFn = extern "C" fn(*const CoopContext) -> i64;

    #[link(name = "i13c", kind = "static")]
    extern "C" {
        fn coop_init(coop: *mut CoopInfo, submissions: u32) -> i64;
        fn coop_free(coop: *const CoopInfo) -> i64;
        fn coop_spawn(coop: *const CoopInfo, ptr: CoopFn, ctx: *const CoopContext) -> i64;
        fn coop_loop(coop: *const CoopInfo) -> i64;
    }

    #[test]
    fn can_initialize_coop() {
        let mut coop = CoopInfo::default();

        unsafe {
            assert_eq!(0, coop_init(&mut coop, 32));
            assert_eq!(0, coop_free(&coop));
        }
    }

    #[test]
    fn can_spawn_one_synchronous_task_and_loop_through_it() {
        let mut val: u64 = 0;
        let mut coop = CoopInfo::default();

        let ctx = CoopContext::new(&mut val);
        let ptr = &ctx as *const CoopContext;

        extern "C" fn task_fn(ctx: *const CoopContext) -> i64 {
            unsafe {
                (*ctx).add(42);
                return 0;
            }
        }

        unsafe {
            assert_eq!(0, coop_init(&mut coop, 32));
            assert_eq!(0, coop_spawn(&coop, task_fn, ptr));
            assert_eq!(0, coop_loop(&coop));
            assert_eq!(0, coop_free(&coop));
        }

        assert_eq!(42, ctx.get());
    }

    #[test]
    fn can_spawn_two_synchronous_tasks_and_loop_through_them() {
        let mut val: u64 = 0;
        let mut coop = CoopInfo::default();

        let ctx = CoopContext::new(&mut val);
        let ptr = &ctx as *const CoopContext;

        extern "C" fn task_one(ctx: *const CoopContext) -> i64 {
            unsafe {
                (*ctx).add(13);
                return 0;
            }
        }

        extern "C" fn task_two(ctx: *const CoopContext) -> i64 {
            unsafe {
                (*ctx).add(17);
                return 0;
            }
        }

        unsafe {
            assert_eq!(0, coop_init(&mut coop, 32));
            assert_eq!(0, coop_spawn(&coop, task_one, ptr));
            assert_eq!(0, coop_spawn(&coop, task_two, ptr));
            assert_eq!(0, coop_loop(&coop));
            assert_eq!(0, coop_free(&coop));
        }

        assert_eq!(30, ctx.get());
    }
}
