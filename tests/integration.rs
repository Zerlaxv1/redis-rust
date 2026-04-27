use std::io::{Read, Write};
use std::net::TcpStream;
use std::process::{Child, Command};
use std::thread;
use std::time::Duration;

fn resp_encode(args: &[&str]) -> Vec<u8> {
    let mut buf = format!("*{}\r\n", args.len()).into_bytes();
    for arg in args {
        buf.extend(format!("${}\r\n{}\r\n", arg.len(), arg).into_bytes());
    }
    buf
}

fn send_resp(stream: &mut TcpStream, args: &[&str]) -> String {
    let encoded = resp_encode(args);
    stream.write_all(&encoded).unwrap();
    stream.flush().unwrap();

    thread::sleep(Duration::from_millis(50));

    let mut buf = [0u8; 4096];
    let n = stream.read(&mut buf).unwrap();
    String::from_utf8_lossy(&buf[..n]).to_string()
}

fn start_server() -> Child {
    let child = Command::new("cargo")
        .args(["run", "--release", "--quiet"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .spawn()
        .expect("failed to start server");

    thread::sleep(Duration::from_secs(1));
    child
}

fn connect() -> TcpStream {
    let stream = TcpStream::connect("127.0.0.1:6379").expect("failed to connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    stream
}

// ==================== Basic Commands ====================

#[test]
fn test_01_ping() {
    let mut server = start_server();
    let mut stream = connect();

    let resp = send_resp(&mut stream, &["PING"]);
    assert_eq!(resp, "+PONG\r\n");

    server.kill().ok();
}

#[test]
fn test_02_multiple_pings() {
    let mut server = start_server();
    let mut stream = connect();

    for _ in 0..3 {
        let resp = send_resp(&mut stream, &["PING"]);
        assert_eq!(resp, "+PONG\r\n");
    }

    server.kill().ok();
}

#[test]
fn test_03_concurrent_clients() {
    let mut server = start_server();

    let mut c1 = connect();
    let mut c2 = connect();
    let mut c3 = connect();

    assert_eq!(send_resp(&mut c1, &["PING"]), "+PONG\r\n");
    assert_eq!(send_resp(&mut c2, &["PING"]), "+PONG\r\n");
    assert_eq!(send_resp(&mut c1, &["PING"]), "+PONG\r\n");
    assert_eq!(send_resp(&mut c1, &["PING"]), "+PONG\r\n");
    assert_eq!(send_resp(&mut c2, &["PING"]), "+PONG\r\n");
    assert_eq!(send_resp(&mut c3, &["PING"]), "+PONG\r\n");

    server.kill().ok();
}

#[test]
fn test_04_echo() {
    let mut server = start_server();
    let mut stream = connect();

    let resp = send_resp(&mut stream, &["ECHO", "apple"]);
    assert_eq!(resp, "$5\r\napple\r\n");

    server.kill().ok();
}

// ==================== SET / GET ====================

#[test]
fn test_05_set_get() {
    let mut server = start_server();
    let mut stream = connect();

    let resp = send_resp(&mut stream, &["SET", "banana", "grape"]);
    assert_eq!(resp, "+OK\r\n");

    let resp = send_resp(&mut stream, &["GET", "banana"]);
    assert_eq!(resp, "$5\r\ngrape\r\n");

    server.kill().ok();
}

#[test]
fn test_06_expiry() {
    let mut server = start_server();
    let mut stream = connect();

    let resp = send_resp(&mut stream, &["SET", "mango", "apple", "PX", "100"]);
    assert_eq!(resp, "+OK\r\n");

    let resp = send_resp(&mut stream, &["GET", "mango"]);
    assert_eq!(resp, "$5\r\napple\r\n");

    thread::sleep(Duration::from_millis(150));

    let resp = send_resp(&mut stream, &["GET", "mango"]);
    assert_eq!(resp, "$-1\r\n");

    server.kill().ok();
}

// ==================== Lists ====================

#[test]
fn test_07_rpush_create() {
    let mut server = start_server();
    let mut stream = connect();

    let resp = send_resp(&mut stream, &["RPUSH", "mylist", "orange"]);
    assert_eq!(resp, ":1\r\n");

    server.kill().ok();
}

#[test]
fn test_08_rpush_append() {
    let mut server = start_server();
    let mut stream = connect();

    assert_eq!(send_resp(&mut stream, &["RPUSH", "mylist", "pear"]), ":1\r\n");
    assert_eq!(send_resp(&mut stream, &["RPUSH", "mylist", "grape"]), ":2\r\n");
    assert_eq!(send_resp(&mut stream, &["RPUSH", "mylist", "banana"]), ":3\r\n");

    server.kill().ok();
}

#[test]
fn test_09_rpush_multiple() {
    let mut server = start_server();
    let mut stream = connect();

    let resp = send_resp(&mut stream, &["RPUSH", "mylist", "raspberry", "blueberry"]);
    assert_eq!(resp, ":2\r\n");

    let resp = send_resp(
        &mut stream,
        &["RPUSH", "mylist", "strawberry", "raspberry", "blueberry"],
    );
    assert_eq!(resp, ":5\r\n");

    server.kill().ok();
}

#[test]
fn test_10_lrange_positive() {
    let mut server = start_server();
    let mut stream = connect();

    send_resp(
        &mut stream,
        &["RPUSH", "apple", "orange", "raspberry", "pineapple", "mango"],
    );

    assert_eq!(
        send_resp(&mut stream, &["LRANGE", "apple", "0", "1"]),
        "*2\r\n$6\r\norange\r\n$9\r\nraspberry\r\n"
    );
    assert_eq!(
        send_resp(&mut stream, &["LRANGE", "apple", "1", "3"]),
        "*3\r\n$9\r\nraspberry\r\n$9\r\npineapple\r\n$5\r\nmango\r\n"
    );
    assert_eq!(
        send_resp(&mut stream, &["LRANGE", "apple", "1", "0"]),
        "*0\r\n"
    );
    assert_eq!(
        send_resp(&mut stream, &["LRANGE", "apple", "0", "8"]),
        "*4\r\n$6\r\norange\r\n$9\r\nraspberry\r\n$9\r\npineapple\r\n$5\r\nmango\r\n"
    );
    assert_eq!(
        send_resp(&mut stream, &["LRANGE", "missing_key", "0", "1"]),
        "*0\r\n"
    );

    server.kill().ok();
}

#[test]
fn test_11_lrange_negative() {
    let mut server = start_server();
    let mut stream = connect();

    send_resp(
        &mut stream,
        &["RPUSH", "fruits", "apple", "pineapple", "grape", "orange"],
    );

    assert_eq!(
        send_resp(&mut stream, &["LRANGE", "fruits", "0", "-3"]),
        "*2\r\n$5\r\napple\r\n$9\r\npineapple\r\n"
    );
    assert_eq!(
        send_resp(&mut stream, &["LRANGE", "fruits", "-3", "-1"]),
        "*3\r\n$9\r\npineapple\r\n$5\r\ngrape\r\n$6\r\norange\r\n"
    );
    assert_eq!(
        send_resp(&mut stream, &["LRANGE", "fruits", "0", "-1"]),
        "*4\r\n$5\r\napple\r\n$9\r\npineapple\r\n$5\r\ngrape\r\n$6\r\norange\r\n"
    );
    assert_eq!(
        send_resp(&mut stream, &["LRANGE", "fruits", "-1", "-2"]),
        "*0\r\n"
    );
    assert_eq!(
        send_resp(&mut stream, &["LRANGE", "fruits", "-5", "-1"]),
        "*4\r\n$5\r\napple\r\n$9\r\npineapple\r\n$5\r\ngrape\r\n$6\r\norange\r\n"
    );

    server.kill().ok();
}

#[test]
fn test_12_lpush() {
    let mut server = start_server();
    let mut stream = connect();

    assert_eq!(send_resp(&mut stream, &["LPUSH", "mylist", "strawberry"]), ":1\r\n");
    assert_eq!(
        send_resp(&mut stream, &["LPUSH", "mylist", "raspberry", "mango"]),
        ":3\r\n"
    );
    assert_eq!(
        send_resp(&mut stream, &["LRANGE", "mylist", "0", "-1"]),
        "*3\r\n$5\r\nmango\r\n$9\r\nraspberry\r\n$10\r\nstrawberry\r\n"
    );

    server.kill().ok();
}

#[test]
fn test_13_llen() {
    let mut server = start_server();
    let mut stream = connect();

    send_resp(
        &mut stream,
        &["RPUSH", "mylist", "banana", "pear", "orange", "blueberry", "apple"],
    );

    assert_eq!(send_resp(&mut stream, &["LLEN", "mylist"]), ":5\r\n");
    assert_eq!(send_resp(&mut stream, &["LLEN", "missing_key"]), ":0\r\n");

    server.kill().ok();
}

#[test]
fn test_14_lpop_single() {
    let mut server = start_server();
    let mut stream = connect();

    send_resp(
        &mut stream,
        &["RPUSH", "mylist", "grape", "banana", "pear", "blueberry", "orange"],
    );

    assert_eq!(send_resp(&mut stream, &["LPOP", "mylist"]), "$5\r\ngrape\r\n");
    assert_eq!(
        send_resp(&mut stream, &["LRANGE", "mylist", "0", "-1"]),
        "*4\r\n$6\r\nbanana\r\n$4\r\npear\r\n$9\r\nblueberry\r\n$6\r\norange\r\n"
    );

    server.kill().ok();
}

#[test]
fn test_15_lpop_multiple() {
    let mut server = start_server();
    let mut stream = connect();

    send_resp(
        &mut stream,
        &["RPUSH", "mylist", "orange", "apple", "pear", "banana", "raspberry"],
    );

    assert_eq!(
        send_resp(&mut stream, &["LPOP", "mylist", "3"]),
        "*3\r\n$6\r\norange\r\n$5\r\napple\r\n$4\r\npear\r\n"
    );
    assert_eq!(
        send_resp(&mut stream, &["LRANGE", "mylist", "0", "-1"]),
        "*2\r\n$6\r\nbanana\r\n$9\r\nraspberry\r\n"
    );

    server.kill().ok();
}

// ==================== BLPOP ====================

#[test]
fn test_16_blpop() {
    let mut server = start_server();

    let handle = thread::spawn(|| {
        let mut stream = connect();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        send_resp(&mut stream, &["BLPOP", "blocklist", "0"])
    });

    thread::sleep(Duration::from_millis(500));

    let mut stream2 = connect();
    assert_eq!(send_resp(&mut stream2, &["RPUSH", "blocklist", "hello"]), ":1\r\n");

    let blpop_resp = handle.join().unwrap();
    assert_eq!(
        blpop_resp,
        "*2\r\n$9\r\nblocklist\r\n$5\r\nhello\r\n"
    );

    server.kill().ok();
}

#[test]
fn test_17_blpop_timeout_expires() {
    let mut server = start_server();
    let mut stream = connect();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    let resp = send_resp(&mut stream, &["BLPOP", "nolist", "1"]);
    assert_eq!(resp, "*-1\r\n");

    server.kill().ok();
}

#[test]
fn test_18_blpop_timeout_with_push() {
    let mut server = start_server();

    let handle = thread::spawn(|| {
        let mut stream = connect();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        send_resp(&mut stream, &["BLPOP", "tlist", "5"])
    });

    thread::sleep(Duration::from_millis(500));

    let mut stream2 = connect();
    send_resp(&mut stream2, &["RPUSH", "tlist", "foo"]);

    let blpop_resp = handle.join().unwrap();
    assert_eq!(
        blpop_resp,
        "*2\r\n$5\r\ntlist\r\n$3\r\nfoo\r\n"
    );

    server.kill().ok();
}

// ==================== TYPE command ====================

#[test]
fn test_19_type_string() {
    let mut server = start_server();
    let mut stream = connect();

    send_resp(&mut stream, &["SET", "some_key", "foo"]);

    assert_eq!(send_resp(&mut stream, &["TYPE", "some_key"]), "+string\r\n");
    assert_eq!(send_resp(&mut stream, &["TYPE", "missing_key"]), "+none\r\n");

    server.kill().ok();
}

#[test]
fn test_20_type_list() {
    let mut server = start_server();
    let mut stream = connect();

    send_resp(&mut stream, &["RPUSH", "mylist", "a"]);

    assert_eq!(send_resp(&mut stream, &["TYPE", "mylist"]), "+list\r\n");

    server.kill().ok();
}

// ==================== Streams - XADD ====================

#[test]
fn test_21_xadd_create() {
    let mut server = start_server();
    let mut stream = connect();

    let resp = send_resp(&mut stream, &["XADD", "stream_key", "0-1", "foo", "bar"]);
    assert_eq!(resp, "$3\r\n0-1\r\n");

    assert_eq!(send_resp(&mut stream, &["TYPE", "stream_key"]), "+stream\r\n");

    server.kill().ok();
}

// ==================== Streams - Validate entry IDs ====================

#[test]
fn test_22_xadd_validate_ids() {
    let mut server = start_server();
    let mut stream = connect();

    assert_eq!(
        send_resp(&mut stream, &["XADD", "s", "1-1", "foo", "bar"]),
        "$3\r\n1-1\r\n"
    );
    assert_eq!(
        send_resp(&mut stream, &["XADD", "s", "1-2", "bar", "baz"]),
        "$3\r\n1-2\r\n"
    );
    assert_eq!(
        send_resp(&mut stream, &["XADD", "s", "1-2", "baz", "foo"]),
        "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"
    );
    assert_eq!(
        send_resp(&mut stream, &["XADD", "s", "0-3", "baz", "foo"]),
        "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"
    );
    assert_eq!(
        send_resp(&mut stream, &["XADD", "s", "0-0", "baz", "foo"]),
        "-ERR The ID specified in XADD must be greater than 0-0\r\n"
    );

    server.kill().ok();
}

// ==================== Streams - Partially auto-generated IDs ====================

#[test]
fn test_23_xadd_partial_auto_id() {
    let mut server = start_server();
    let mut stream = connect();

    assert_eq!(
        send_resp(&mut stream, &["XADD", "s", "0-*", "foo", "bar"]),
        "$3\r\n0-1\r\n"
    );
    assert_eq!(
        send_resp(&mut stream, &["XADD", "s", "5-*", "foo", "bar"]),
        "$3\r\n5-0\r\n"
    );
    assert_eq!(
        send_resp(&mut stream, &["XADD", "s", "5-*", "bar", "baz"]),
        "$3\r\n5-1\r\n"
    );

    server.kill().ok();
}

// ==================== Streams - Fully auto-generated IDs ====================

#[test]
fn test_24_xadd_full_auto_id() {
    let mut server = start_server();
    let mut stream = connect();

    let resp = send_resp(&mut stream, &["XADD", "s", "*", "foo", "bar"]);
    assert!(resp.starts_with('$'), "expected bulk string, got: {}", resp);
    assert!(resp.contains('-'), "expected ID with dash, got: {}", resp);

    server.kill().ok();
}

// ==================== Streams - XRANGE ====================

#[test]
fn test_25_xrange() {
    let mut server = start_server();
    let mut stream = connect();

    send_resp(&mut stream, &["XADD", "s", "0-1", "foo", "bar"]);
    send_resp(&mut stream, &["XADD", "s", "0-2", "bar", "baz"]);
    send_resp(&mut stream, &["XADD", "s", "0-3", "baz", "foo"]);

    let resp = send_resp(&mut stream, &["XRANGE", "s", "0-2", "0-3"]);
    assert_eq!(
        resp,
        "*2\r\n*2\r\n$3\r\n0-2\r\n*2\r\n$3\r\nbar\r\n$3\r\nbaz\r\n*2\r\n$3\r\n0-3\r\n*2\r\n$3\r\nbaz\r\n$3\r\nfoo\r\n"
    );

    server.kill().ok();
}

#[test]
fn test_26_xrange_start_dash() {
    let mut server = start_server();
    let mut stream = connect();

    send_resp(&mut stream, &["XADD", "s", "0-1", "foo", "bar"]);
    send_resp(&mut stream, &["XADD", "s", "0-2", "bar", "baz"]);
    send_resp(&mut stream, &["XADD", "s", "0-3", "baz", "foo"]);

    let resp = send_resp(&mut stream, &["XRANGE", "s", "-", "0-2"]);
    assert_eq!(
        resp,
        "*2\r\n*2\r\n$3\r\n0-1\r\n*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n*2\r\n$3\r\n0-2\r\n*2\r\n$3\r\nbar\r\n$3\r\nbaz\r\n"
    );

    server.kill().ok();
}

#[test]
fn test_27_xrange_end_plus() {
    let mut server = start_server();
    let mut stream = connect();

    send_resp(&mut stream, &["XADD", "s", "0-1", "foo", "bar"]);
    send_resp(&mut stream, &["XADD", "s", "0-2", "bar", "baz"]);
    send_resp(&mut stream, &["XADD", "s", "0-3", "baz", "foo"]);

    let resp = send_resp(&mut stream, &["XRANGE", "s", "0-2", "+"]);
    assert_eq!(
        resp,
        "*2\r\n*2\r\n$3\r\n0-2\r\n*2\r\n$3\r\nbar\r\n$3\r\nbaz\r\n*2\r\n$3\r\n0-3\r\n*2\r\n$3\r\nbaz\r\n$3\r\nfoo\r\n"
    );

    server.kill().ok();
}

// ==================== Streams - XREAD ====================

#[test]
fn test_28_xread_single() {
    let mut server = start_server();
    let mut stream = connect();

    send_resp(&mut stream, &["XADD", "s", "0-1", "temperature", "96"]);

    let resp = send_resp(&mut stream, &["XREAD", "STREAMS", "s", "0-0"]);
    assert_eq!(
        resp,
        "*1\r\n*2\r\n$1\r\ns\r\n*1\r\n*2\r\n$3\r\n0-1\r\n*2\r\n$11\r\ntemperature\r\n$2\r\n96\r\n"
    );

    server.kill().ok();
}

#[test]
fn test_29_xread_multiple() {
    let mut server = start_server();
    let mut stream = connect();

    send_resp(&mut stream, &["XADD", "s1", "0-1", "temperature", "95"]);
    send_resp(&mut stream, &["XADD", "s2", "0-2", "humidity", "97"]);

    let resp = send_resp(
        &mut stream,
        &["XREAD", "STREAMS", "s1", "s2", "0-0", "0-1"],
    );
    assert_eq!(
        resp,
        "*2\r\n*2\r\n$2\r\ns1\r\n*1\r\n*2\r\n$3\r\n0-1\r\n*2\r\n$11\r\ntemperature\r\n$2\r\n95\r\n*2\r\n$2\r\ns2\r\n*1\r\n*2\r\n$3\r\n0-2\r\n*2\r\n$8\r\nhumidity\r\n$2\r\n97\r\n"
    );

    server.kill().ok();
}

// ==================== Streams - XREAD blocking ====================

#[test]
fn test_30_xread_block_with_data() {
    let mut server = start_server();
    let mut stream1 = connect();

    send_resp(&mut stream1, &["XADD", "s", "0-1", "temperature", "96"]);

    let handle = thread::spawn(|| {
        let mut stream = connect();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        send_resp(
            &mut stream,
            &["XREAD", "BLOCK", "1000", "STREAMS", "s", "0-1"],
        )
    });

    thread::sleep(Duration::from_millis(500));

    let mut stream2 = connect();
    send_resp(&mut stream2, &["XADD", "s", "0-2", "temperature", "95"]);

    let resp = handle.join().unwrap();
    assert_eq!(
        resp,
        "*1\r\n*2\r\n$1\r\ns\r\n*1\r\n*2\r\n$3\r\n0-2\r\n*2\r\n$11\r\ntemperature\r\n$2\r\n95\r\n"
    );

    server.kill().ok();
}

#[test]
fn test_31_xread_block_timeout() {
    let mut server = start_server();
    let mut stream = connect();

    send_resp(&mut stream, &["XADD", "s", "0-1", "temperature", "96"]);

    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let resp = send_resp(
        &mut stream,
        &["XREAD", "BLOCK", "1000", "STREAMS", "s", "0-1"],
    );
    assert_eq!(resp, "*-1\r\n");

    server.kill().ok();
}

// ==================== Streams - XREAD blocking without timeout ====================

#[test]
fn test_32_xread_block_indefinite() {
    let mut server = start_server();
    let mut stream1 = connect();

    send_resp(&mut stream1, &["XADD", "s", "0-1", "temperature", "96"]);

    let handle = thread::spawn(|| {
        let mut stream = connect();
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        send_resp(
            &mut stream,
            &["XREAD", "BLOCK", "0", "STREAMS", "s", "0-1"],
        )
    });

    thread::sleep(Duration::from_millis(1500));

    let mut stream2 = connect();
    send_resp(&mut stream2, &["XADD", "s", "0-2", "temperature", "95"]);

    let resp = handle.join().unwrap();
    assert_eq!(
        resp,
        "*1\r\n*2\r\n$1\r\ns\r\n*1\r\n*2\r\n$3\r\n0-2\r\n*2\r\n$11\r\ntemperature\r\n$2\r\n95\r\n"
    );

    server.kill().ok();
}

// ==================== Streams - XREAD blocking with $ ====================

#[test]
fn test_33_xread_block_dollar() {
    let mut server = start_server();
    let mut stream1 = connect();

    send_resp(&mut stream1, &["XADD", "s", "0-1", "temperature", "96"]);

    let handle = thread::spawn(|| {
        let mut stream = connect();
        stream
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        send_resp(
            &mut stream,
            &["XREAD", "BLOCK", "0", "STREAMS", "s", "$"],
        )
    });

    thread::sleep(Duration::from_millis(500));

    let mut stream2 = connect();
    send_resp(&mut stream2, &["XADD", "s", "0-2", "temperature", "95"]);

    let resp = handle.join().unwrap();
    assert_eq!(
        resp,
        "*1\r\n*2\r\n$1\r\ns\r\n*1\r\n*2\r\n$3\r\n0-2\r\n*2\r\n$11\r\ntemperature\r\n$2\r\n95\r\n"
    );

    server.kill().ok();
}

#[test]
fn test_34_xread_block_dollar_timeout() {
    let mut server = start_server();
    let mut stream = connect();

    send_resp(&mut stream, &["XADD", "s", "0-1", "temperature", "96"]);

    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let resp = send_resp(
        &mut stream,
        &["XREAD", "BLOCK", "1000", "STREAMS", "s", "$"],
    );
    assert_eq!(resp, "*-1\r\n");

    server.kill().ok();
}
