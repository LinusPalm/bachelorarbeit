from dataclasses import dataclass
import datetime
from json import JSONDecodeError, dumps, load, loads
import logging
import socket
import threading
import time
from typing import Callable, Dict, List, Optional, Set, Tuple
from typing_extensions import Literal
from threading import Thread
import selectors

from metrics import Metrics

ControlTypes = Literal["change_inputs", "engine_ready", "engine_done"]

def parse_ip(raw: str):
    parts = raw.split(':')

    return (parts[0], int(parts[1]))

class ControlMessage():
    type: ControlTypes
    params: List[str]
    sourceNode: str
    destNode: str
    forwardNode: Optional[str]

    def __init__(self, type: ControlTypes, params: List[str], sourceNode: str, destNode: str, forwardNode: Optional[str] = None) -> None:
        self.type = type
        self.params = params
        self.sourceNode = sourceNode
        self.destNode = destNode
        self.forwardNode = forwardNode

    def __repr__(self) -> str:
        return f"ControlMessage(type={self.type}, params={self.params}, from={self.sourceNode}, to={self.destNode}, via={self.forwardNode})"

    @staticmethod
    def parse(jsonStr: bytes):
        json = loads(jsonStr)
        return ControlMessage(json["type"], json["params"], json["sourceNode"], json["destNode"], json["forwardNode"])

ControlMessageReceiver = Callable[[ControlMessage], None]

@dataclass
class TopologyConfigNode():
    id: str
    ip_address: str
    port: int
    control_port: int
    forwarding_table: Dict[str, Tuple[str, int]] # Key: node id, Value: (ip, port)
    connections: List[Tuple[str, int]]

class TopologyConfig:
    def __init__(self, id_to_node: Dict[str, TopologyConfigNode], ip_to_node: Dict[Tuple[str, int], TopologyConfigNode]) -> None:
        self.nodes = id_to_node
        self.ip_to_node = ip_to_node

    @staticmethod
    def parse(path: str, id_filter: Optional[Set[str]] = None):
        with open(path, "r") as f:
            json: dict = load(f)

        ip_map: Dict[Tuple[str, int], TopologyConfigNode] = {}
        nodes: Dict[str, TopologyConfigNode] = {}
        for nodeId, node in json.items():
            if id_filter != None and nodeId not in id_filter:
                continue

            ip = node["ip"]
            port = int(node["port"])
            control_port = int(node["control_port"])
            forwarding = { toNode: parse_ip(toIp) for toNode, toIp in node["forwarding_table"].items() if id_filter == None or toNode in id_filter }
            connections = [ parse_ip(address) for address in node["connection"] ]

            nodes[nodeId] = TopologyConfigNode(nodeId, ip, port, control_port, forwarding, connections)
            ip_map[(ip, port)] = nodes[nodeId]

        if id_filter != None:
            def is_in_filter(ip_and_port: Tuple[str, int]):
                node = ip_map.get(ip_and_port, None)
                return node != None and node.id in id_filter

            for node in nodes.values():
                node.connections = list(filter(is_in_filter, node.connections))

        return TopologyConfig(nodes, ip_map)

class ControlMessageService():
    __BUFFER_SIZE = 256
    __MSG_TERMINATOR = b'\n'

    def __init__(self, nodeId: str, topology: TopologyConfig, metrics: Metrics) -> None:
        self.nodeId = nodeId
        self.topology = topology
        self.forwarding = topology.nodes[nodeId].forwarding_table
        self.is_listening = False
        self.is_cancelled = False
        self.sent_broadcasts: Set[Tuple[str, str]] = set()
        self.__control_sockets: Dict[Tuple[str, int], socket.socket] = {}
        self.__send_lock = threading.Lock()
        
        self.__receivers: Dict[str, List[ControlMessageReceiver]] = {}
        self.__total_count = metrics.get_traffic_counter("control_message")
        self.__message_counts = {
            "change_inputs": metrics.get_traffic_counter("change_inputs"),
            "engine_ready": metrics.get_traffic_counter("engine_ready"),
            "engine_done": metrics.get_traffic_counter("engine_done")
        }

    def __send(self, message: ControlMessage, dest: Tuple[str, int]):
        with self.__send_lock:
            message.forwardNode = self.nodeId

            if dest not in self.__control_sockets:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                retries = 0
                while retries < 4:
                    try:
                        s.connect(dest)
                        break
                    except ConnectionError as e:
                        logging.warn(f"Failed to connect control socket to {dest}: {e}")
                        retries += 1
                        if retries > 3:
                            raise e
                        
                        time.sleep(5)
                
                logging.debug(f"Connected control socket to {dest}")
                self.__control_sockets[dest] = s
            else:
                s = self.__control_sockets[dest]

            # Terminate message, so receiver can distinguish multiple messages in same received buffer
            msg_json = dumps(message.__dict__)
            s.sendall(msg_json.encode() + self.__MSG_TERMINATOR)

            self.__total_count.inc_sent()
            self.__message_counts[message.type].inc_sent()

    def send_control_message(self, message: ControlMessage):
        """
        Sends control message to next node on the path to `message.destNode`
        """
        print(f"[ControlMessageService] Sending ({message.type}, {message.params}) to {message.destNode}")

        destIp = self.forwarding[message.destNode][0]
        destPort = self.topology.nodes[message.destNode].control_port

        self.__send(message, (destIp, destPort))

    def start_listening(self, port: int):
        """
        Start listening for incoming control messages and report them to registered receivers.
        """
        self.is_cancelled = False
        self.listenThread = Thread(target=self.listen, args=(port,))
        self.listenThread.start()

    def broadcast_ready(self):
        logging.debug(f"[ControlMessageService] {datetime.datetime.now()}: Broadcasting engine_ready for this node")
        msg = ControlMessage("engine_ready", [], self.nodeId, "BROADCAST")
        self.broadcast_message(msg)

        return msg

    def broadcast_message(self, msg: ControlMessage):
        forward_node = msg.forwardNode # Copy, __send modifies msg.forwardNode
        msg.destNode = "BROADCAST"
        for conn in self.topology.nodes[self.nodeId].connections:
            node = self.topology.ip_to_node[conn]
            if node.id != forward_node and node.id != msg.sourceNode:
                self.__send(msg, (conn[0], node.control_port))
                logging.debug(f"Forwarding broadcast {msg} to {node.id}")
            else:
                logging.debug(f"Not forwarding broadcast {msg} back to source")

    def close(self):
        """
        Stop listening for incoming control messages.
        """
        self.is_cancelled = True
        for s in self.__control_sockets.values():
            s.close()

    def add_receiver(self, message_type: ControlTypes, receiver: ControlMessageReceiver):
        """
        Add a function that is called, when a control message of type `message_type` is received by this instance.
        """
        if message_type not in self.__receivers:
            self.__receivers[message_type] = []

        self.__receivers[message_type].append(receiver)

    def remove_receiver(self, message_type: ControlTypes, receiver: ControlMessageReceiver):
        """
        Remove a previously added control message receiver function.
        """
        if message_type in self.__receivers and receiver in self.__receivers[message_type]:
            self.__receivers[message_type].remove(receiver)

    def listen(self, port: int):
        own_ip = self.topology.nodes[self.nodeId].ip_address
        print(f"[ControlMessageService] {datetime.datetime.now()}: Listening for control messages on {own_ip}:{port}")

        selector = selectors.DefaultSelector()

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.bind((own_ip, port))
        s.listen(1)
        s.setblocking(False)

        def listen_read(connection: socket.socket, mask):
            data = connection.recv(self.__BUFFER_SIZE)
            if data:
                while not data.endswith(self.__MSG_TERMINATOR):
                    data += connection.recv(self.__BUFFER_SIZE)

                all_messages = data.split(self.__MSG_TERMINATOR)
                for msg_data in all_messages:
                    if len(msg_data) > 0:
                        try:
                            message = ControlMessage.parse(msg_data)
                            self.on_received(message)
                        except JSONDecodeError:
                            logging.warn("[ControlMessageService] Invalid control message", msg_data)
            else:
                selector.unregister(connection)
                connection.close()

        def listen_accept(sock: socket.socket, mask):
            connection, remoteAddr = sock.accept()
            connection.setblocking(False)
            selector.register(connection, selectors.EVENT_READ, listen_read)

        selector.register(s, selectors.EVENT_READ, listen_accept)

        self.is_listening = True

        while not self.is_cancelled:
            # Use timeout, so listen thread can be stopped gracefully
            events = selector.select(timeout=2)
            for key, mask in events:
                callback = key.data
                callback(key.fileobj, mask)

        self.is_listening = False
        print("[ControlMessageService] Done listening.")

    def on_received(self, received: ControlMessage):
        logging.debug(f"[ControlMessageService] {datetime.datetime.now()}: Received ({received.type}, {received.params}) for {received.destNode} from {received.sourceNode} (via {received.forwardNode})")
        if received.destNode == "BROADCAST":
            messageId = (received.sourceNode, received.type)
            if messageId not in self.sent_broadcasts:
                self.sent_broadcasts.add(messageId)
                self.broadcast_message(received)
            else:
                logging.debug(f"Dropping duplicate broadcast message {messageId}")
        else:
            if received.destNode != self.nodeId:
                logging.debug(f"[ControlMessageService] Forwarding control message {received.type} for node {received.destNode}")
                self.send_control_message(received)

        if received.type in self.__receivers:
            for handler in self.__receivers[received.type]:
                handler(received)

        self.__total_count.inc_received()
        self.__message_counts[received.type].inc_received()

    def __exit__(self, *args):
        self.close()

    def __enter__(self, *args):
        return self
