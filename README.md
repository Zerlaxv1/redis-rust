# Redis Clone in Rust

A Redis server built in Rust with Tokio, implementing the RESP protocol from scratch.

## Commands implemented

| Command | Description |
|---------|-------------|
| PING | Returns PONG |
| ECHO | Returns the argument |
| SET | Set key to value, supports PX/EX expiry |
| GET | Get value by key, respects expiry |
| RPUSH | Append elements to a list |
| LPUSH | Prepend elements to a list |
| LRANGE | Get range of elements from a list (supports negative indexes) |
| LLEN | Get list length |
| LPOP | Remove and return elements from the head |
| BLPOP | Blocking pop with timeout support |

## Architecture

```
src/
  main.rs         -- TCP server, command dispatcher, command implementations, RESP response helpers
  resp_parser.rs  -- RESP protocol decoder (implements tokio_util::codec::Decoder)
tests/
  integration.rs  -- integration tests (reproducing CodeCrafters test suite)
```

## Run

```sh
cargo run
```

## Test

```sh
cargo test -- --test-threads=1
```

Tests must run sequentially (`--test-threads=1`) since all tests share port 6379.
