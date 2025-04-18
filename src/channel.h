#pragma once
#include "coop.h"

typedef struct {
    long data[33];
} channel_info;

/// @brief initializes hand-off channel
/// @param channel ptr to uninitialized structure
/// @param coop ptr to already initialized coop info structure
/// @param participants number of participants in the channel
/// @return 0 if no error, or negative value indicating an error
extern long channel_init(channel_info* channel, coop_info* coop, unsigned long participants);

/// @brief sends a message to the channel
/// @param channel ptr to channel structure
/// @param data ptr to message or just a message
/// @return 0 if no error, or negative value indicating an error
extern long channel_send(channel_info* channel, const void* data);

/// @brief receives a message from the channel
/// @param channel ptr to channel structure
/// @param data ptr to a space where the message will be stored
/// @return 0 if no error, or negative value indicating an error
extern long channel_recv(channel_info* channel, void** data);
