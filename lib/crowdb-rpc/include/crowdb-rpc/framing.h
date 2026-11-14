// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "crowdb-rpc/buffer.h"

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <new>
#include <vector>

namespace crowdb::rpc
{
class Connection;

// ── Wire format (14-byte header) ──────────────────────────────────
//
// [magic:2][msg_type:2][msg_size:4][data_size:4][msg_offset:1][flags:1]
//
// Little-endian, field-by-field (not memcpy of the struct — avoids
// compiler-layout dependence). See design-crowdb-rpc.md §3 for
// the rationale behind each field and what was removed from the
// reference's 20-byte header.

constexpr uint16_t MAGIC       = 0xCA70;
constexpr uint8_t  HEADER_SIZE = 14;

// flags bit definitions
constexpr uint8_t FLAG_ONE_WAY = 0x01;

struct Header
{
    uint16_t magic      = MAGIC;
    uint16_t msg_type   = 0;
    uint32_t msg_size   = 0;           // control message length
    uint32_t data_size  = 0;           // data payload length
    uint8_t  msg_offset = HEADER_SIZE; // offset to control message
    uint8_t  flags      = 0;
};

// Direct socket reads overwrite the resized control bytes. The parser
// publishes the frame only after it has filled the entire control.
template <class T> struct ControlByteAllocator : std::allocator<T>
{
    template <class U> struct rebind
    {
        using other = ControlByteAllocator<U>;
    };

    void construct(T *location) noexcept
    {
        ::new (static_cast<void *>(location)) T;
    }
};

using ControlBytes = std::vector<uint8_t, ControlByteAllocator<uint8_t>>;

// A complete frame owns the control bytes and any pool-allocated data.
// The handler that receives a Frame* deletes it when done.
struct Frame
{
    Header   header;
    uint64_t request_id                        = 0;       // extracted from control during parse
    uint64_t rpc_create_nano                   = 0;       // extracted from control during parse
    uint64_t response_create_nano              = 0;       // extracted from control during parse (responses only)
    Buffer  *data_buf                          = nullptr; // pool-allocated; nullptr if control-only
    void (*release_callback)(void *, Buffer *) = nullptr;
    void *release_context                      = nullptr;

    // Raw control message bytes (flatbuffer). Populated for all frames with
    // msg_size > 0. Common handlers (ping, unknown) use only request_id +
    // rpc_create_nano and ignore this; service-specific handlers (diskio)
    // parse the full flatbuffer from here.
    // Optional logical-control admission retained through the final caller.
    // Storage is destroyed before admission wakes blocked receivers.
    std::shared_ptr<void> control_owner;
    // Asynchronous FFI handlers retain their connection through Frame release.
    std::shared_ptr<Connection> connection_owner;
    uint64_t                    tcp_request_cache_bytes       = 0;
    bool                        release_admission_on_delivery = false;
    ControlBytes                control;

    ~Frame();
};

enum class FramingError {
    None = 0,
    BadMagic,
    BadOffset,
    DataTooLarge,
};

// Serialize a header into a 12-byte buffer (little-endian, field-by-field).
void serialize_header(uint8_t *buf, const Header &h);

// Parse a 12-byte buffer into a header (little-endian, field-by-field).
Header parse_header(const uint8_t *buf);

// Extract request_id + rpc_create_nano + response_create_nano from a
// flatbuffer control message. All common messages
// (ConnectionPingRequest/Response, UnknownMessage) share the same
// layout for these fields. response_create_nano is 0 for requests.
void extract_control_fields(const uint8_t *control, uint32_t len, uint64_t &out_request_id,
                            uint64_t &out_rpc_create_nano, uint64_t &out_response_create_nano);

// ── FrameParser — pull-based zero-copy state machine ──────────────
//
// The parser drives receive-side zero-copy. Instead of reading into a
// scratch buffer and copying, it tells the read loop *where to read
// next* — directly into pool-allocated Buffers. This unifies the TCP and
// RDMA receive paths.
//
// The parser extracts routing fields from the completed control and retains
// its bytes in the frame. Data is read into a pool-allocated Buffer.
//
// Usage in the read loop:
//   target = parser.next_read_target();
//   n = read(fd, target.ptr, target.len);
//   frame = parser.advance(n);
//   if (frame) dispatch(frame);
//
// For batched reads (header + control from recv_buf):
//   consumed = parser.feed_data(recv_buf, n, on_frame);
//   // feed_data stops at ReadingData — read loop handles data directly.

enum class ParseState {
    ReadingHeader,
    ReadingControl,
    ReadingData,
};

class FrameParser
{
  public:
    struct ReadTarget
    {
        uint8_t *ptr = nullptr;
        uint32_t len = 0;
    };

    explicit FrameParser(uint32_t max_data_size = 4 << 20);

    // Set the pool for data buffer allocation (called by Connection).
    void set_pool(BufferPool *p)
    {
        pool_ = p;
    }

    [[nodiscard]] ParseState state() const
    {
        return state_;
    }

    [[nodiscard]] uint32_t control_remaining() const
    {
        return state_ == ParseState::ReadingControl ? header_.msg_size - control_offset_ : 0;
    }

    // Where to read next. len == 0 means no more bytes needed right now.
    ReadTarget next_read_target();

    // Mark n bytes as consumed. Transitions state, allocates the next
    // buffer if needed. Returns a complete Frame* when the frame is done,
    // or nullptr if more bytes are needed. On error, sets error_ and
    // returns nullptr (caller should check last_error()).
    Frame *advance(uint32_t bytes_read);

    // Feed bytes from an external buffer (e.g. a per-worker receive
    // buffer filled by one big read()). Processes header + control,
    // yields complete control-only frames via the callback. Stops when
    // the parser enters ReadingData state — the read loop handles data
    // directly (separate read for data into pool Buffer).
    // Returns the number of bytes consumed.
    template <typename Callback> uint32_t feed_data(const uint8_t *data, uint32_t len, Callback &&on_frame)
    {
        uint32_t consumed = 0;
        while (consumed < len && error_ == FramingError::None) {
            // Stop at data — the read loop handles data directly.
            if (state_ == ParseState::ReadingData) {
                break;
            }
            const auto target = next_read_target();
            if (target.len == 0) {
                break;
            }
            const auto to_copy = std::min(target.len, len - consumed);
            std::memcpy(target.ptr, data + consumed, to_copy);
            consumed += to_copy;
            Frame *frame = advance(to_copy);
            if (frame) {
                on_frame(frame);
            }
        }
        return consumed;
    }

    // Reset to ReadingHeader (after a frame is yielded or on error).
    void reset();

    [[nodiscard]] FramingError last_error() const
    {
        return error_;
    }

  private:
    uint32_t     max_data_size_;
    ParseState   state_ = ParseState::ReadingHeader;
    FramingError error_ = FramingError::None;

    Header   header_;
    uint8_t  header_buf_[HEADER_SIZE];
    uint32_t header_offset_ = 0;

    uint32_t control_offset_ = 0;
    uint32_t data_offset_    = 0;

    // Pool for data allocation.
    BufferPool *pool_ = nullptr;

    // Extracted during parse (stored in Frame on yield).
    uint64_t parsed_request_id_           = 0;
    uint64_t parsed_rpc_create_nano_      = 0;
    uint64_t parsed_response_create_nano_ = 0;

    // Owns partial control and pool data until advance yields a complete frame.
    std::unique_ptr<Frame> frame_;

    // Validate the parsed header; returns FramingError::None if ok.
    [[nodiscard]] FramingError validate_header() const;

    // Build and return the completed Frame; reset state for next frame.
    Frame *yield_frame();
};

} // namespace crowdb::rpc
