import socket

import argh


def send_items(data):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(("0.0.0.0", 80))
    s.send(data.encode("utf-8"))


if __name__ == "__main__":
    argh.dispatch_command(send_items)
