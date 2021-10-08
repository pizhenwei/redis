RDMA Support
============

Getting Started
---------------

## Building

To build with RDMA support you'll need RDMA development libraries (e.g.
librdmacm-dev and libibverbs-dev on Debian/Ubuntu).

Run `make BUILD_RDMA=yes`.

## Running manually

To manually run a Redis server with RDMA mode:

    ./src/redis-server --rdma-port 6379  --rdma-bind 192.168.122.100 \
        --protected-mode no

To connect to this Redis server with `redis-cli`:

    ./src/redis-cli -h 192.168.122.100 --rdma

It's also possible to have both RDMA and TCP available, and there is no
conflict of TCP(6379) and RDMA(6379), Ex:

    ./src/redis-server --rdma-port 6379  --rdma-bind 192.168.122.100 \
         --port 6379 --protected-mode no

Note that the network card (192.168.122.100 of this example) should support
RDMA. To test a server supports RDMA or not:
    ~# rdma res show

Connections
-----------

RDMA operations also go through a connection abstraction layer that hides
I/O and read/write event handling from the caller.

Because RDMA is a message-oriented protocol, Redis works under a stream-oriented
protocol, to implement the stream-like operations(Ex, read/write) needs
additional works in RDMA scenario.

## Protocol
In Redis, separate control-plane(to exchange control message) and data-plane(to
transfer the real payload for Redis).

### Control message
For control message, use a 32 bytes message which defines a structure:
```
typedef struct RedisRdmaCmd {
    uint8_t magic;
    uint8_t version;
    uint8_t opcode;
    uint8_t rsvd[13];
    uint64_t addr;
    uint32_t length;
    uint32_t key;
} RedisRdmaCmd;

magic   : should be 'R'
version : 1
opcode  : defined as following enum RedisRdmaOpcode
rsvd    : reserved for further extension.
addr    : local address of a buffer which is used to receive remote streaming
          data, aka 'RX buffer address'. The remote side should use this
          address as 'TX buffer address'.
length  : length of the 'RX buffer'.
key     : the RDMA remote key of 'RX buffer'.
```


```
typedef enum RedisRdmaOpcode {
    RegisterLocalAddr,
} RedisRdmaOpcode;

RegisterLocalAddr : tell the 'RX buffer' information to the remote side, and the
                    remote side uses this as 'TX buffer'
```

#### Operations of RDMA
- To send a control message by RDMA opcode '**IBV_WR_SEND**' with structure
  'RedisRdmaCmd'.
- To receive a control message by RDMA '**ibv_post_recv**', and the received buffer
  size should be size of 'RedisRdmaCmd'.
- To transfer stream data by RDMA opcode '**IBV_WR_RDMA_WRITE_WITH_IMM**'.


### Maximum CQE(s) of RDMA
No specific restriction is defined in this protocol. Recommended CQEs should be
more than the elements of 'COMMAND' response to avoid RDMA flush error. Currently
both Redis server and hiredis use 1024 as default.


### The workflow of this protocol
```
                                                                  server
                                                                  listen RDMA port
          client
                -------------------RDMA connect------------------>
                                                                  accept connection
                <--------------- Establish RDMA ------------------
                                                                  setup RX buffer
                <----- Register Local buffer [@IBV_WR_SEND] ------
[@ibv_post_recv]
setup TX buffer
                ------ Register Local buffer [@IBV_WR_SEND] ----->
                                                                  [@ibv_post_recv]
                                                                  setup TX buffer
                -- Redis commands [@IBV_WR_RDMA_WRITE_WITH_IMM] ->
                <- Redis response [@IBV_WR_RDMA_WRITE_WITH_IMM] --
                                  .......
                -- Redis commands [@IBV_WR_RDMA_WRITE_WITH_IMM] ->
                <- Redis response [@IBV_WR_RDMA_WRITE_WITH_IMM] --
                                  .......


RX is full
                ------ Register Local buffer [@IBV_WR_SEND] ----->
                                                                  [@ibv_post_recv]
                                                                  setup TX buffer
                <- Redis response [@IBV_WR_RDMA_WRITE_WITH_IMM] --
                                  .......

                                                                  RX is full
                <----- Register Local buffer [@IBV_WR_SEND] ------
[@ibv_post_recv]
setup TX buffer
                -- Redis commands [@IBV_WR_RDMA_WRITE_WITH_IMM] ->
                <- Redis response [@IBV_WR_RDMA_WRITE_WITH_IMM] --
                                  .......

                ------------------RDMA disconnect---------------->
                <-----------------RDMA disconnect-----------------
```


## Event handling
There is no POLLOUT event of RDMA comp channel:
   1, if TX is not full, it's always writable.
   2, if TX is full, should wait a 'RegisterLocalAddr' message to refresh
      'TX buffer'.

To-Do List
----------
- [ ] rdma client & benchmark
- [ ] POLLOUT event emulation for hiredis
- [ ] auto-test suite is not implemented currently
