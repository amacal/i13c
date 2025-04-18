use crate::coop::CoopInfo;

#[repr(C)]
pub struct ChannelInfo {
    data: [u64; 5],
}

impl Default for ChannelInfo {
    fn default() -> Self {
        Self { data: [0; 5] }
    }
}

#[link(name = "i13c", kind = "static")]
extern "C" {
    pub fn channel_init(channel: *mut ChannelInfo, coop: *const CoopInfo, size: usize) -> i64;
    pub fn channel_free(channel: *const ChannelInfo) -> i64;

    pub fn channel_send(channel: *const ChannelInfo, data: i64) -> i64;
    pub fn channel_recv(channel: *const ChannelInfo, data: *mut i64) -> i64;
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
            assert_eq!(0, channel_free(&channel));
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
                let mut data: i64 = 0;

                (*ctx).add(13);
                (*ctx).add(channel_recv((*ctx).channel(), &mut data));
                (*ctx).add(data);

                return 0;
            }
        }

        extern "C" fn task_two(ctx: *const CoopContext) -> i64 {
            unsafe {
                (*ctx).add(17);
                (*ctx).add(channel_send((*ctx).channel(), 19));

                return 0;
            }
        }

        unsafe {
            assert_eq!(0, coop_init(&mut coop, 32));
            assert_eq!(0, channel_init(&mut channel, &coop, 3));
            assert_eq!(0, coop_spawn(&coop, task_one, ptr, 0));
            assert_eq!(0, coop_spawn(&coop, task_two, ptr, 0));
            assert_eq!(0, coop_loop(&coop));
            assert_eq!(0, channel_free(&channel));
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
                let mut data: i64 = 0;
                (*ctx).add(13);

                (*ctx).add(channel_recv((*ctx).channel(), &mut data));
                (*ctx).add(data);

                (*ctx).add(channel_recv((*ctx).channel(), &mut data));
                (*ctx).add(data);

                return 0;
            }
        }

        extern "C" fn task_two(ctx: *const CoopContext) -> i64 {
            unsafe {
                (*ctx).add(17);
                (*ctx).add(channel_send((*ctx).channel(), 19));
                (*ctx).add(channel_send((*ctx).channel(), 21));

                return 0;
            }
        }

        unsafe {
            assert_eq!(0, coop_init(&mut coop, 32));
            assert_eq!(0, channel_init(&mut channel, &coop, 3));
            assert_eq!(0, coop_spawn(&coop, task_one, ptr, 0));
            assert_eq!(0, coop_spawn(&coop, task_two, ptr, 0));
            assert_eq!(0, coop_loop(&coop));
            assert_eq!(0, channel_free(&channel));
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
                let mut data: i64 = 0;

                (*ctx).add(13);
                (*ctx).add(channel_recv((*ctx).channel(), &mut data));
                (*ctx).add(data);

                return 0;
            }
        }

        extern "C" fn task_two(ctx: *const CoopContext) -> i64 {
            unsafe {
                (*ctx).add(17);
                (*ctx).add(channel_send((*ctx).channel(), 19));
                (*ctx).add(channel_send((*ctx).channel(), 21));

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
            assert_eq!(0, channel_free(&channel));
            assert_eq!(0, coop_free(&coop));
        }

        assert_eq!(val, 83);
    }
}
