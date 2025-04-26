use crate::coop::CoopInfo;

#[repr(C)]
pub struct ChannelInfo {
    data: [u64; 9],
}

impl Default for ChannelInfo {
    fn default() -> Self {
        Self { data: [0; 9] }
    }
}

#[link(name = "i13c", kind = "static")]
extern "C" {
    pub fn channel_init(channel: *mut ChannelInfo, coop: *const CoopInfo, size: u64) -> i64;
    pub fn channel_free(channel: *const ChannelInfo, flags: u64) -> i64;

    pub fn channel_send(channel: *const ChannelInfo, data: *const i64) -> i64;
    pub fn channel_recv(channel: *const ChannelInfo, data: *mut *const i64) -> i64;

    pub fn channel_select(channel: *const *const ChannelInfo, data: *mut *const i64) -> i64;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coop::*;

    #[test]
    fn can_initialize_channel() {
        let mut coop = CoopInfo::default();
        let mut channel = ChannelInfo::default();

        unsafe {
            assert_eq!(0, coop_init(&mut coop, 32));
            assert_eq!(0, channel_init(&mut channel, &coop, 1));
            assert_eq!(0, channel_free(&channel, 1));
            assert_eq!(0, coop_free(&coop));
        }
    }

    #[test]
    fn can_handle_one_receiver_then_one_sender() {
        let mut val: i64 = 0;

        let mut coop = CoopInfo::default();
        let mut channel = ChannelInfo::default();

        let ctx = CoopContext::new(&mut val, &coop).with_channel(&channel);
        let ptr = &ctx as *const CoopContext;

        extern "C" fn task_one(ctx: *const CoopContext) -> i64 {
            unsafe {
                let mut ptr: *const i64 = core::ptr::null();

                (*ctx).add(13);
                (*ctx).add(channel_recv((*ctx).channel(), &mut ptr));
                (*ctx).add(*ptr);

                (*ctx).add(channel_free((*ctx).channel(), 0));
                return 0;
            }
        }

        extern "C" fn task_two(ctx: *const CoopContext) -> i64 {
            unsafe {
                let data: [i64; 3] = [19; 3];
                let ptr = data.as_ptr();

                (*ctx).add(17);
                (*ctx).add(channel_send((*ctx).channel(), ptr));

                (*ctx).add(channel_free((*ctx).channel(), 0));
                return 0;
            }
        }

        unsafe {
            assert_eq!(0, coop_init(&mut coop, 32));
            assert_eq!(0, channel_init(&mut channel, &coop, 3));
            assert_eq!(0, coop_spawn(&coop, task_one, ptr, 0));
            assert_eq!(0, coop_spawn(&coop, task_two, ptr, 0));
            assert_eq!(0, coop_loop(&coop));
            assert_eq!(0, channel_free(&channel, 0));
            assert_eq!(0, coop_free(&coop));
        }

        assert_eq!(val, 49);
    }

    #[test]
    fn can_handle_one_receiver_then_one_sender_twice() {
        let mut val: i64 = 0;

        let mut coop = CoopInfo::default();
        let mut channel = ChannelInfo::default();

        let ctx = CoopContext::new(&mut val, &coop).with_channel(&channel);
        let ptr = &ctx as *const CoopContext;

        extern "C" fn task_one(ctx: *const CoopContext) -> i64 {
            unsafe {
                let mut ptr: *const i64 = core::ptr::null();

                (*ctx).add(13);

                (*ctx).add(channel_recv((*ctx).channel(), &mut ptr));
                (*ctx).add(*ptr);

                (*ctx).add(channel_recv((*ctx).channel(), &mut ptr));
                (*ctx).add(*ptr);

                (*ctx).add(channel_free((*ctx).channel(), 0));
                return 0;
            }
        }

        extern "C" fn task_two(ctx: *const CoopContext) -> i64 {
            unsafe {
                let data1: [i64; 3] = [19; 3];
                let ptr1 = data1.as_ptr();

                let data2: [i64; 3] = [21; 3];
                let ptr2 = data2.as_ptr();

                (*ctx).add(17);
                (*ctx).add(channel_send((*ctx).channel(), ptr1));
                (*ctx).add(channel_send((*ctx).channel(), ptr2));

                (*ctx).add(channel_free((*ctx).channel(), 0));
                return 0;
            }
        }

        unsafe {
            assert_eq!(0, coop_init(&mut coop, 32));
            assert_eq!(0, channel_init(&mut channel, &coop, 3));
            assert_eq!(0, coop_spawn(&coop, task_one, ptr, 0));
            assert_eq!(0, coop_spawn(&coop, task_two, ptr, 0));
            assert_eq!(0, coop_loop(&coop));
            assert_eq!(0, channel_free(&channel, 0));
            assert_eq!(0, coop_free(&coop));
        }

        assert_eq!(val, 70);
    }

    #[test]
    fn can_handle_two_receivers_then_one_sender() {
        let mut val: i64 = 0;

        let mut coop = CoopInfo::default();
        let mut channel = ChannelInfo::default();

        let ctx = CoopContext::new(&mut val, &coop).with_channel(&channel);
        let ptr = &ctx as *const CoopContext;

        extern "C" fn task_one(ctx: *const CoopContext) -> i64 {
            unsafe {
                let mut ptr: *const i64 = core::ptr::null();

                (*ctx).add(13);
                (*ctx).add(channel_recv((*ctx).channel(), &mut ptr));
                (*ctx).add(*ptr);

                (*ctx).add(channel_free((*ctx).channel(), 0));
                return 0;
            }
        }

        extern "C" fn task_two(ctx: *const CoopContext) -> i64 {
            unsafe {
                let data1: [i64; 3] = [19; 3];
                let ptr1 = data1.as_ptr();

                let data2: [i64; 3] = [21; 3];
                let ptr2 = data2.as_ptr();

                (*ctx).add(17);
                (*ctx).add(channel_send((*ctx).channel(), ptr1));
                (*ctx).add(channel_send((*ctx).channel(), ptr2));

                (*ctx).add(channel_free((*ctx).channel(), 0));
                return 0;
            }
        }

        unsafe {
            assert_eq!(0, coop_init(&mut coop, 32));
            assert_eq!(0, channel_init(&mut channel, &coop, 4));
            assert_eq!(0, coop_spawn(&coop, task_one, ptr, 0));
            assert_eq!(0, coop_spawn(&coop, task_one, ptr, 0));
            assert_eq!(0, coop_spawn(&coop, task_two, ptr, 0));
            assert_eq!(0, coop_loop(&coop));
            assert_eq!(0, channel_free(&channel, 0));
            assert_eq!(0, coop_free(&coop));
        }

        assert_eq!(val, 83);
    }

    #[test]
    fn can_handle_one_sender_then_one_receiver() {
        let mut val: i64 = 0;

        let mut coop = CoopInfo::default();
        let mut channel = ChannelInfo::default();

        let ctx = CoopContext::new(&mut val, &coop).with_channel(&channel);
        let ptr = &ctx as *const CoopContext;

        extern "C" fn task_one(ctx: *const CoopContext) -> i64 {
            unsafe {
                let data: [i64; 3] = [19; 3];
                let ptr = data.as_ptr();

                (*ctx).add(17);
                (*ctx).add(channel_send((*ctx).channel(), ptr));

                (*ctx).add(channel_free((*ctx).channel(), 0));
                return 0;
            }
        }

        extern "C" fn task_two(ctx: *const CoopContext) -> i64 {
            unsafe {
                let mut ptr: *const i64 = core::ptr::null();

                (*ctx).add(13);
                (*ctx).add(channel_recv((*ctx).channel(), &mut ptr));
                (*ctx).add(*ptr);

                (*ctx).add(channel_free((*ctx).channel(), 0));
                return 0;
            }
        }

        unsafe {
            assert_eq!(0, coop_init(&mut coop, 32));
            assert_eq!(0, channel_init(&mut channel, &coop, 3));
            assert_eq!(0, coop_spawn(&coop, task_one, ptr, 0));
            assert_eq!(0, coop_spawn(&coop, task_two, ptr, 0));
            assert_eq!(0, coop_loop(&coop));
            assert_eq!(0, channel_free(&channel, 0));
            assert_eq!(0, coop_free(&coop));
        }

        assert_eq!(val, 49);
    }

    #[test]
    fn can_handle_one_sender_then_one_receiver_twice() {
        let mut val: i64 = 0;

        let mut coop = CoopInfo::default();
        let mut channel = ChannelInfo::default();

        let ctx = CoopContext::new(&mut val, &coop).with_channel(&channel);
        let ptr = &ctx as *const CoopContext;

        extern "C" fn task_one(ctx: *const CoopContext) -> i64 {
            unsafe {
                let data1: [i64; 3] = [19; 3];
                let ptr1 = data1.as_ptr();

                let data2: [i64; 3] = [21; 3];
                let ptr2 = data2.as_ptr();

                (*ctx).add(17);
                (*ctx).add(channel_send((*ctx).channel(), ptr1));
                (*ctx).add(channel_send((*ctx).channel(), ptr2));

                (*ctx).add(channel_free((*ctx).channel(), 0));
                return 0;
            }
        }

        extern "C" fn task_two(ctx: *const CoopContext) -> i64 {
            unsafe {
                let mut ptr: *const i64 = core::ptr::null();

                (*ctx).add(13);

                (*ctx).add(channel_recv((*ctx).channel(), &mut ptr));
                (*ctx).add(*ptr);

                (*ctx).add(channel_recv((*ctx).channel(), &mut ptr));
                (*ctx).add(*ptr);

                (*ctx).add(channel_free((*ctx).channel(), 0));
                return 0;
            }
        }

        unsafe {
            assert_eq!(0, coop_init(&mut coop, 32));
            assert_eq!(0, channel_init(&mut channel, &coop, 3));
            assert_eq!(0, coop_spawn(&coop, task_one, ptr, 0));
            assert_eq!(0, coop_spawn(&coop, task_two, ptr, 0));
            assert_eq!(0, coop_loop(&coop));
            assert_eq!(0, channel_free(&channel, 0));
            assert_eq!(0, coop_free(&coop));
        }

        assert_eq!(val, 70);
    }

    #[test]
    fn can_handle_two_senders_then_one_receiver() {
        let mut val: i64 = 0;

        let mut coop = CoopInfo::default();
        let mut channel = ChannelInfo::default();

        let ctx = CoopContext::new(&mut val, &coop).with_channel(&channel);
        let ptr = &ctx as *const CoopContext;

        extern "C" fn task_one(ctx: *const CoopContext) -> i64 {
            unsafe {
                let data1: [i64; 3] = [19; 3];
                let ptr1 = data1.as_ptr();

                (*ctx).add(17);
                (*ctx).add(channel_send((*ctx).channel(), ptr1));

                (*ctx).add(channel_free((*ctx).channel(), 0));
                return 0;
            }
        }

        extern "C" fn task_two(ctx: *const CoopContext) -> i64 {
            unsafe {
                let mut ptr: *const i64 = core::ptr::null();

                (*ctx).add(13);

                (*ctx).add(channel_recv((*ctx).channel(), &mut ptr));
                (*ctx).add(*ptr);

                (*ctx).add(channel_recv((*ctx).channel(), &mut ptr));
                (*ctx).add(*ptr);

                (*ctx).add(channel_free((*ctx).channel(), 0));
                return 0;
            }
        }

        unsafe {
            assert_eq!(0, coop_init(&mut coop, 32));
            assert_eq!(0, channel_init(&mut channel, &coop, 4));
            assert_eq!(0, coop_spawn(&coop, task_one, ptr, 0));
            assert_eq!(0, coop_spawn(&coop, task_one, ptr, 0));
            assert_eq!(0, coop_spawn(&coop, task_two, ptr, 0));
            assert_eq!(0, coop_loop(&coop));
            assert_eq!(0, channel_free(&channel, 0));
            assert_eq!(0, coop_free(&coop));
        }

        assert_eq!(val, 85);
    }

    #[test]
    fn can_handle_coordinator_freed_channel() {
        let mut val: i64 = 0;

        let mut coop = CoopInfo::default();

        let ctx = CoopContext::new(&mut val, &coop);
        let ptr = &ctx as *const CoopContext;

        extern "C" fn coordinator(ctx: *const CoopContext) -> i64 {
            unsafe {
                let mut channel = ChannelInfo::default();
                let ctx = (*ctx).with_channel(&channel);
                let ptr = &ctx as *const CoopContext;

                ctx.add(1);
                ctx.add(channel_init(&mut channel, ctx.coop(), 3));
                ctx.add(coop_spawn(ctx.coop(), task_one, ptr, 0));
                ctx.add(coop_spawn(ctx.coop(), task_two, ptr, 0));
                ctx.add(channel_free(&channel, 1));
            }

            return 0;
        }

        extern "C" fn task_one(ctx: *const CoopContext) -> i64 {
            unsafe {
                let mut ptr: *const i64 = core::ptr::null();

                (*ctx).add(13);
                (*ctx).add(channel_recv((*ctx).channel(), &mut ptr));
                (*ctx).add(*ptr);

                (*ctx).add(channel_free((*ctx).channel(), 0));
                return 0;
            }
        }

        extern "C" fn task_two(ctx: *const CoopContext) -> i64 {
            unsafe {
                let data: [i64; 3] = [19; 3];
                let ptr = data.as_ptr();

                (*ctx).add(17);
                (*ctx).add(channel_send((*ctx).channel(), ptr));

                (*ctx).add(channel_free((*ctx).channel(), 0));
                return 0;
            }
        }

        unsafe {
            assert_eq!(0, coop_init(&mut coop, 32));
            assert_eq!(0, coop_spawn(&coop, coordinator, ptr, 0));
            assert_eq!(0, coop_loop(&coop));
            assert_eq!(0, coop_free(&coop));
        }

        assert_eq!(val, 50);
    }

    #[test]
    fn can_multiselect_ready_channel_first() {
        let mut val: i64 = 0;

        let mut coop = CoopInfo::default();

        let ctx = CoopContext::new(&mut val, &coop);
        let ptr = &ctx as *const CoopContext;

        extern "C" fn coordinator(ctx: *const CoopContext) -> i64 {
            unsafe {
                let ctx = *ctx;

                let mut ch1 = ChannelInfo::default();
                let ctx1 = ctx.with_channel(&ch1);
                let ptr1 = &ctx1 as *const CoopContext;

                let mut ch2 = ChannelInfo::default();
                let ctx2 = ctx.with_channel(&ch2);
                let ptr2 = &ctx2 as *const CoopContext;

                let mut ptr: *const i64 = core::ptr::null();
                let channels: [*const ChannelInfo; 3] = [&ch1, &ch2, core::ptr::null()];

                ctx.add(1);
                ctx.add(channel_init(&mut ch1, ctx.coop(), 2));
                ctx.add(channel_init(&mut ch2, ctx.coop(), 2));

                ctx.add(coop_spawn(ctx.coop(), task_one, ptr1, 0));
                ctx.add(coop_spawn(ctx.coop(), task_two, ptr2, 0));

                ctx.add(coop_noop(ctx.coop()));
                ctx.add(channel_select(channels.as_ptr(), &mut ptr));
                ctx.add(*ptr);

                ctx.add(channel_select(channels.as_ptr(), &mut ptr));
                ctx.add(channel_free(&ch1, 1));
            }

            return 0;
        }

        extern "C" fn task_one(ctx: *const CoopContext) -> i64 {
            unsafe {
                let data: [i64; 3] = [17; 3];
                let ptr = data.as_ptr();

                (*ctx).add(*ptr);
                (*ctx).add(channel_send((*ctx).channel(), ptr));
                (*ctx).add(channel_free((*ctx).channel(), 0));
                return 0;
            }
        }

        extern "C" fn task_two(ctx: *const CoopContext) -> i64 {
            unsafe {
                let data: [i64; 3] = [19; 3];
                let ptr = data.as_ptr();

                (*ctx).add(*ptr);
                (*ctx).add(channel_send((*ctx).channel(), ptr));
                (*ctx).add(channel_free((*ctx).channel(), 0));
                return 0;
            }
        }

        unsafe {
            assert_eq!(0, coop_init(&mut coop, 32));
            assert_eq!(0, coop_spawn(&coop, coordinator, ptr, 0));
            assert_eq!(0, coop_loop(&coop));
            assert_eq!(0, coop_free(&coop));
        }

        assert_eq!(val, 55);
    }

    #[test]
    fn can_multiselect_waiting_channel_first() {
        let mut val: i64 = 0;
        let mut coop = CoopInfo::default();

        let ctx = CoopContext::new(&mut val, &coop);
        let ptr = &ctx as *const CoopContext;

        extern "C" fn coordinator(ctx: *const CoopContext) -> i64 {
            unsafe {
                let ctx = *ctx;

                let mut ch1 = ChannelInfo::default();
                let ctx1 = ctx.with_channel(&ch1);
                let ptr1 = &ctx1 as *const CoopContext;

                let mut ch2 = ChannelInfo::default();
                let ctx2 = ctx.with_channel(&ch2);
                let ptr2 = &ctx2 as *const CoopContext;

                let mut ptr: *const i64 = core::ptr::null();
                let channels: [*const ChannelInfo; 3] = [&ch1, &ch2, core::ptr::null()];

                ctx.add(1);
                ctx.add(channel_init(&mut ch1, ctx.coop(), 2));
                ctx.add(channel_init(&mut ch2, ctx.coop(), 2));

                ctx.add(coop_spawn(ctx.coop(), task_one, ptr1, 0));
                ctx.add(coop_spawn(ctx.coop(), task_two, ptr2, 0));

                ctx.add(channel_select(channels.as_ptr(), &mut ptr));
                ctx.add(*ptr);

                ctx.add(channel_select(channels.as_ptr(), &mut ptr));
                ctx.add(channel_free(&ch1, 1));
            }

            return 0;
        }

        extern "C" fn task_one(ctx: *const CoopContext) -> i64 {
            unsafe {
                let data: [i64; 3] = [17; 3];
                let ptr = data.as_ptr();

                (*ctx).add(*ptr);
                (*ctx).add(channel_send((*ctx).channel(), ptr));
                (*ctx).add(channel_free((*ctx).channel(), 0));
                return 0;
            }
        }

        extern "C" fn task_two(ctx: *const CoopContext) -> i64 {
            unsafe {
                let data: [i64; 3] = [19; 3];
                let ptr = data.as_ptr();

                (*ctx).add(*ptr);
                (*ctx).add(channel_send((*ctx).channel(), ptr));
                (*ctx).add(channel_free((*ctx).channel(), 0));
                return 0;
            }
        }

        unsafe {
            assert_eq!(0, coop_init(&mut coop, 32));
            assert_eq!(0, coop_spawn(&coop, coordinator, ptr, 0));
            assert_eq!(0, coop_loop(&coop));
            assert_eq!(0, coop_free(&coop));
        }

        assert_eq!(val, 55);
    }

    #[test]
    fn can_multiselect_waiting_channel_second() {
        let mut val: i64 = 0;
        let mut coop = CoopInfo::default();

        let ctx = CoopContext::new(&mut val, &coop);
        let ptr = &ctx as *const CoopContext;

        extern "C" fn coordinator(ctx: *const CoopContext) -> i64 {
            unsafe {
                let ctx = *ctx;

                let mut ch1 = ChannelInfo::default();
                let ctx1 = ctx.with_channel(&ch1);
                let ptr1 = &ctx1 as *const CoopContext;

                let mut ch2 = ChannelInfo::default();
                let ctx2 = ctx.with_channel(&ch2);
                let ptr2 = &ctx2 as *const CoopContext;

                let mut ptr: *const i64 = core::ptr::null();
                let channels: [*const ChannelInfo; 3] = [&ch1, &ch2, core::ptr::null()];

                ctx.add(1);
                ctx.add(channel_init(&mut ch1, ctx.coop(), 2));
                ctx.add(channel_init(&mut ch2, ctx.coop(), 2));

                ctx.add(coop_spawn(ctx.coop(), receiver, ptr2, 0));
                ctx.add(coop_spawn(ctx.coop(), sender, ptr1, 0));

                ctx.add(channel_select(channels.as_ptr(), &mut ptr));
                ctx.add(*ptr);

                ctx.add(channel_select(channels.as_ptr(), &mut ptr));
                ctx.add(channel_free(&ch1, 1));
            }

            return 0;
        }

        extern "C" fn sender(ctx: *const CoopContext) -> i64 {
            unsafe {
                let data: [i64; 3] = [17; 3];
                let ptr = data.as_ptr();

                (*ctx).add(*ptr);
                (*ctx).add(channel_send((*ctx).channel(), ptr));
                (*ctx).add(channel_free((*ctx).channel(), 0));
                return 0;
            }
        }

        extern "C" fn receiver(ctx: *const CoopContext) -> i64 {
            unsafe {
                let data: [i64; 3] = [19; 3];
                let ptr = data.as_ptr();

                (*ctx).add(*ptr);
                (*ctx).add(channel_send((*ctx).channel(), ptr));
                (*ctx).add(channel_free((*ctx).channel(), 0));
                return 0;
            }
        }

        unsafe {
            assert_eq!(0, coop_init(&mut coop, 32));
            assert_eq!(0, coop_spawn(&coop, coordinator, ptr, 0));
            assert_eq!(0, coop_loop(&coop));
            assert_eq!(0, coop_free(&coop));
        }

        assert_eq!(val, 57);
    }

    #[test]
    fn can_multiselect_waiting_channel_unlink() {
        let mut val: i64 = 0;
        let mut coop = CoopInfo::default();

        let ctx = CoopContext::new(&mut val, &coop);
        let ptr = &ctx as *const CoopContext;

        extern "C" fn coordinator(ctx: *const CoopContext) -> i64 {
            unsafe {
                let ctx = *ctx;

                let mut ch1 = ChannelInfo::default();
                let ctx1 = ctx.with_channel(&ch1);
                let ptr1 = &ctx1 as *const CoopContext;

                let mut ch2 = ChannelInfo::default();
                let ctx2 = ctx.with_channel(&ch2);
                let ptr2 = &ctx2 as *const CoopContext;

                let mut ptr: *const i64 = core::ptr::null();
                let channels: [*const ChannelInfo; 3] = [&ch1, &ch2, core::ptr::null()];

                ctx.next(0, 0, channel_init(&mut ch1, ctx.coop(), 4));
                ctx.next(1, 0, channel_init(&mut ch2, ctx.coop(), 2));

                // it will cause an instant channel-recv 1
                ctx.next(2, 0, coop_spawn(ctx.coop(), receiver, ptr1, 0));
                ctx.next(3, 0, coop_noop(ctx.coop()));

                // it will cause a delayed channel-recv 1
                ctx.next(4, 0, coop_spawn(ctx.coop(), receiver, ptr1, 0));

                // it will cause a delayed channel-send 2
                ctx.next(5, 0, coop_spawn(ctx.coop(), sender, ptr2, 0));

                // it will cause a delayed consumption from the channel 2
                // and forced unlinking in the middle of the internal linked list
                ctx.next(7, 1, channel_select(channels.as_ptr(), &mut ptr));
                ctx.next(8, 11, *ptr);

                ctx.next(12, 0, channel_send(&ch1, core::ptr::null()));
                ctx.next(13, 0, channel_send(&ch1, core::ptr::null()));

                ctx.next(14, 0, channel_free(&ch1, 1));
            }

            return 0;
        }

        extern "C" fn receiver(ctx: *const CoopContext) -> i64 {
            unsafe {
                let mut ptr: *const i64 = core::ptr::null();

                (*ctx).add(channel_recv((*ctx).channel(), &mut ptr));
                (*ctx).add(channel_free((*ctx).channel(), 0));

                return 0;
            }
        }

        extern "C" fn sender(ctx: *const CoopContext) -> i64 {
            unsafe {
                let data: [i64; 3] = [11; 3];
                let ptr = data.as_ptr();

                (*ctx).next(6, 0, 0);
                (*ctx).next(9, 0, channel_send((*ctx).channel(), ptr));
                (*ctx).next(10, 0, channel_free((*ctx).channel(), 0));
                (*ctx).next(11, 0, 0);

                return 0;
            }
        }

        unsafe {
            assert_eq!(0, coop_init(&mut coop, 32));
            assert_eq!(0, coop_spawn(&coop, coordinator, ptr, 0));
            assert_eq!(0, coop_loop(&coop));
            assert_eq!(0, coop_free(&coop));
        }

        assert_eq!(val, 14);
    }

    #[test]
    fn can_detect_sender_deadlock() {
        let mut val: i64 = 0;
        let mut coop = CoopInfo::default();

        let ctx = CoopContext::new(&mut val, &coop);
        let ptr = &ctx as *const CoopContext;

        extern "C" fn coordinator(ctx: *const CoopContext) -> i64 {
            unsafe {
                let ctx = *ctx;
                let mut ch = ChannelInfo::default();
                let ctx = ctx.with_channel(&ch);

                let data: [i64; 3] = [11; 3];
                let ptr = data.as_ptr();

                ctx.next(0, 0, channel_init(&mut ch, ctx.coop(), 3));
                ctx.next(1, 0, coop_spawn(ctx.coop(), sender, &ctx, 0));
                ctx.next(2, 0, coop_spawn(ctx.coop(), sender, &ctx, 0));
                ctx.next(3, 0, coop_noop(ctx.coop()));

                ctx.next(4, -35, channel_send(&ch, ptr));
                ctx.next(5, 0, channel_free(&ch, 1));
            }

            return 0;
        }


        extern "C" fn sender(ctx: *const CoopContext) -> i64 {
            unsafe {
                let data: [i64; 3] = [11; 3];
                let ptr = data.as_ptr();

                (*ctx).add(channel_send((*ctx).channel(), ptr));
                (*ctx).add(channel_free((*ctx).channel(), 0));

                return 0;
            }
        }

        unsafe {
            assert_eq!(0, coop_init(&mut coop, 32));
            assert_eq!(0, coop_spawn(&coop, coordinator, ptr, 0));
            assert_eq!(0, coop_loop(&coop));
            assert_eq!(0, coop_free(&coop));
        }

        assert_eq!(val, 5);
    }

    #[test]
    fn can_receiver_sender_deadlock() {
        let mut val: i64 = 0;
        let mut coop = CoopInfo::default();

        let ctx = CoopContext::new(&mut val, &coop);
        let ptr = &ctx as *const CoopContext;

        extern "C" fn coordinator(ctx: *const CoopContext) -> i64 {
            unsafe {
                let ctx = *ctx;
                let mut ch = ChannelInfo::default();
                let ctx = ctx.with_channel(&ch);
                let mut ptr: *const i64 = core::ptr::null();

                ctx.next(0, 0, channel_init(&mut ch, ctx.coop(), 3));
                ctx.next(1, 0, coop_spawn(ctx.coop(), reciver, &ctx, 0));
                ctx.next(2, 0, coop_spawn(ctx.coop(), reciver, &ctx, 0));
                ctx.next(3, 0, coop_noop(ctx.coop()));

                ctx.next(4, -35, channel_recv(&ch, &mut ptr));
                ctx.next(5, 0, channel_free(&ch, 1));
            }

            return 0;
        }


        extern "C" fn reciver(ctx: *const CoopContext) -> i64 {
            unsafe {
                let mut ptr: *const i64 = core::ptr::null();

                (*ctx).add(channel_recv((*ctx).channel(), &mut ptr));
                (*ctx).add(channel_free((*ctx).channel(), 0));

                return 0;
            }
        }

        unsafe {
            assert_eq!(0, coop_init(&mut coop, 32));
            assert_eq!(0, coop_spawn(&coop, coordinator, ptr, 0));
            assert_eq!(0, coop_loop(&coop));
            assert_eq!(0, coop_free(&coop));
        }

        assert_eq!(val, 5);
    }
}
