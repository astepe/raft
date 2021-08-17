import json
import socket
from threading import Thread

from raft_py.raft import RaftCluster


if __name__ == "__main__":
    serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    serversocket.bind(('0.0.0.0', 80))
    serversocket.listen(5)
    raft_cluster = RaftCluster()
    raft_cluster.start()

    def set_on_cluster(**kwargs):
        raft_cluster.set(**kwargs)

    def create_set_thread(clientsocket):
        data = clientsocket.recv(2048)
        payload = json.loads(data)
        return Thread(target= lambda: raft_cluster.set(**payload))

    while True:
        try:
            clientsocket, address = serversocket.accept()
            ct = create_set_thread(clientsocket)
            ct.run()
        except KeyboardInterrupt:
            break

    raft_cluster.stop()
    raft_cluster._events["stopped"].wait()
