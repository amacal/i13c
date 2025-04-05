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
        ptr: *const i64,
        coop: *const CoopInfo,
    }

    impl CoopContext {
        fn new(ptr: *const i64, coop: *const CoopInfo) -> Self {
            CoopContext { ptr, coop }
        }

        fn get(&self) -> i64 {
            unsafe { std::ptr::read_volatile(self.ptr) }
        }

        fn add(&self, val: i64) {
            unsafe {
                std::ptr::write_volatile(
                    self.ptr as *mut i64,
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
        fn coop_noop(coop: *const CoopInfo) -> i64;
        fn coop_timeout(coop: *const CoopInfo, timeout: u32) -> i64;
        fn coop_openat(coop: *const CoopInfo, file_path: *const u8, flags: u32, mode: u32) -> i64;
        fn coop_read(coop: *const CoopInfo, fd: u32, buffer: *mut u8, size: u32, offset: u32) -> i64;
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
        let mut val: i64 = 0;
        let mut coop = CoopInfo::default();

        let ctx = CoopContext::new(&mut val, &coop);
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
        let mut val: i64 = 0;
        let mut coop = CoopInfo::default();

        let ctx = CoopContext::new(&mut val, &coop);
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

    #[test]
    fn can_spawn_one_synchronous_task_and_noop_inside() {
        let mut val: i64 = 0;
        let mut coop = CoopInfo::default();

        let ctx = CoopContext::new(&mut val, &coop);
        let ptr = &ctx as *const CoopContext;

        extern "C" fn task_fn(ctx: *const CoopContext) -> i64 {
            unsafe {
                let res = coop_noop((*ctx).coop);
                (*ctx).add(42 + res);
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
    fn can_spawn_one_synchronous_task_and_wait_inside() {
        let mut val: i64 = 0;
        let mut coop = CoopInfo::default();

        let ctx = CoopContext::new(&mut val, &coop);
        let ptr = &ctx as *const CoopContext;

        extern "C" fn task_fn(ctx: *const CoopContext) -> i64 {
            unsafe {
                let res = coop_timeout((*ctx).coop, 1);
                (*ctx).add(res);
                return 0;
            }
        }

        unsafe {
            assert_eq!(0, coop_init(&mut coop, 32));
            assert_eq!(0, coop_spawn(&coop, task_fn, ptr));
            assert_eq!(0, coop_loop(&coop));
            assert_eq!(0, coop_free(&coop));
        }

        assert_eq!(-62, ctx.get());
    }

    #[test]
    fn can_spawn_one_synchronous_task_and_noop_150_times() {
        let mut val: i64 = 0;
        let mut coop = CoopInfo::default();

        let ctx = CoopContext::new(&mut val, &coop);
        let ptr = &ctx as *const CoopContext;

        extern "C" fn task_fn(ctx: *const CoopContext) -> i64 {
            unsafe {
                for _ in 0..150 {
                    let res = coop_noop((*ctx).coop);
                    (*ctx).add(1 + res);
                }

                return 0;
            }
        }

        unsafe {
            assert_eq!(0, coop_init(&mut coop, 32));
            assert_eq!(0, coop_spawn(&coop, task_fn, ptr));
            assert_eq!(0, coop_loop(&coop));
            assert_eq!(0, coop_free(&coop));
        }

        assert_eq!(150, ctx.get());
    }

    #[test]
    fn can_spawn_150_synchronous_tasks_and_noop_150_times() {
        let mut val: i64 = 0;
        let mut coop = CoopInfo::default();

        let ctx = CoopContext::new(&mut val, &coop);
        let ptr = &ctx as *const CoopContext;

        extern "C" fn task_fn(ctx: *const CoopContext) -> i64 {
            unsafe {
                for _ in 0..150 {
                    let res = coop_noop((*ctx).coop);
                    (*ctx).add(1 + res);
                }

                return 0;
            }
        }

        unsafe {
            assert_eq!(0, coop_init(&mut coop, 32));

            for _ in 0..150 {
                assert_eq!(0, coop_spawn(&coop, task_fn, ptr));
            }

            assert_eq!(0, coop_loop(&coop));
            assert_eq!(0, coop_free(&coop));
        }

        assert_eq!(150 * 150, ctx.get());
    }

    #[test]
    fn can_spawn_one_synchronous_tasks_spawning_150_tasks_to_noop_150_times() {
        let mut val: i64 = 0;
        let mut coop = CoopInfo::default();

        let ctx = CoopContext::new(&mut val, &coop);
        let ptr = &ctx as *const CoopContext;

        extern "C" fn minor_fn(ctx: *const CoopContext) -> i64 {
            unsafe {
                for _ in 0..150 {
                    let res = coop_noop((*ctx).coop);
                    (*ctx).add(1 + res);
                }

                return 0;
            }
        }

        extern "C" fn task_fn(ctx: *const CoopContext) -> i64 {
            unsafe {
                for _ in 0..150 {
                    let res = coop_spawn((*ctx).coop, minor_fn, ctx);
                    (*ctx).add(1 + res);
                }

                return 0;
            }
        }

        unsafe {
            assert_eq!(0, coop_init(&mut coop, 32));
            assert_eq!(0, coop_spawn(&coop, task_fn, ptr));
            assert_eq!(0, coop_loop(&coop));
            assert_eq!(0, coop_free(&coop));
        }

        assert_eq!(150 * 150 + 150, ctx.get());
    }

    #[test]
    fn can_spawn_one_synchronous_task_and_read_makefile() {
        let mut val: i64 = 0;
        let mut coop = CoopInfo::default();

        let ctx = CoopContext::new(&mut val, &coop);
        let ptr = &ctx as *const CoopContext;

        extern "C" fn task_fn(ctx: *const CoopContext) -> i64 {
            let file_path = b"Makefile\0";
            let file_path = file_path.as_ptr();

            let buffer: [u8; 64] = [0; 64];
            let buffer = buffer.as_ptr();

            unsafe {
                let res = coop_openat((*ctx).coop, file_path, 0, 0);
                (*ctx).add(if res < 0 { res } else { 0 });

                let res = coop_read((*ctx).coop, res as u32, buffer as *mut u8, 64, 0);
                (*ctx).add(res);
            }

            return 0;
        }

        unsafe {
            assert_eq!(0, coop_init(&mut coop, 32));
            assert_eq!(0, coop_spawn(&coop, task_fn, ptr));
            assert_eq!(0, coop_loop(&coop));
            assert_eq!(0, coop_free(&coop));
        }

        assert_eq!(64, ctx.get());
    }
}
