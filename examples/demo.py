import socket
import struct


def request(sock, operation, request_id, body):
    sock.sendall(struct.pack(">4sHBBQI", b"KLGN", 1, operation, 0, request_id, len(body)) + body)
    header = sock.recv(20, socket.MSG_WAITALL)
    magic, version, response, flags, returned_id, length = struct.unpack(">4sHBBQI", header)
    if magic != b"KLGN" or version != 1 or returned_id != request_id:
        raise ValueError("unexpected response header")
    data = sock.recv(length, socket.MSG_WAITALL)
    if response == 255:
        code, outcome, reserved, needed, text_length = struct.unpack(">HBBIH", data[:10])
        raise RuntimeError(f"error {code}, outcome {outcome}: {data[10:10 + text_length].decode()}")
    return data


with socket.create_connection(("127.0.0.1", 7676)) as connection:
    response = request(connection, 1, 1, b"\x01hello KubeLog")
    offset, durable_end = struct.unpack(">QQ", response)
    print("appended", offset, "durable end", durable_end)
    response = request(connection, 2, 2, struct.pack(">QII", offset, 1, 1024))
    snapshot_end, next_offset, count = struct.unpack(">QQI", response[:20])
    print("read", count, "record:", response[44:].decode(), "next", next_offset, "snapshot end", snapshot_end)
