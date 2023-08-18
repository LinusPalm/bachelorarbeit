from abc import ABC, abstractmethod
import datetime
import threading
import time
from typing import Callable, List, TextIO
from metrics import Metrics

from node_monitor import NodeMonitor, Event

class OutputReader(ABC):
    def __init__(self, input_stream: TextIO, nodes: List[NodeMonitor], passthrough: bool) -> None:
        self.input_stream = input_stream
        self.nodes = { node.nodeId: node for node in nodes }
        self.passthrough = passthrough

    @abstractmethod
    def read_metadata(self):
        """
        Reads metadata from the engine output before complex event processing starts.
        """
        pass

    @abstractmethod
    def read_event_stream(self):
        """
        Reads generated and received complex events from the engine output and reports them to node monitor(s).
        """
        pass
    

class FlinkEngineOutputReader(OutputReader):
    def __init__(self, input_stream: TextIO, nodes: List[NodeMonitor], passthrough: bool, cancel_event: threading.Event, metrics: Metrics, done_handler: Callable, log_latency = False) -> None:
        if len(nodes) > 1:
            raise ValueError("Flink engine only supports one node per physical node.")

        super().__init__(input_stream, nodes, passthrough)
        self.cancel_event = cancel_event
        self.done_handler = done_handler
        self.log_latency = log_latency
        self.this_node = nodes[0]

        self.__match_count = metrics.get_counter("generated_matches")
        self.__event_count = metrics.get_traffic_counter("events")
        self.__discarded_count = metrics.get_counter("discarded")
        if log_latency:
            self.__latency = metrics.get_value_history(f"latency_{self.this_node.nodeId}")

    def read_metadata(self):
        while not self.cancel_event.is_set():
            line = self.input_stream.readline()
            if line == None or line.strip() == "":
                time.sleep(0.1)
                continue

            line = line.rstrip("\r\n")

            if self.passthrough:
                print(line)

            if line.startswith("[Socket] Listening"):
                break

    def read_event_stream(self):
        while not self.cancel_event.is_set():
            line = self.input_stream.readline()
            if line == None or line.strip() == "":
                time.sleep(0.01)
                continue

            line = line.rstrip("\r\n")

            if self.passthrough:
                print(line)

            if line.startswith("[Match]"):
                # Locally matched events
                # [Match] complex | eventID | timestamp [can be creationTime] | eventType | numberOfEvents | (individual Event);(individual Event)[;...]
                line = line[8:]
                event = Event.from_flink(line, True)
                self.this_node.report_event(event)
                self.__match_count.inc()

                if self.log_latency:
                    parts = line.split(" | ")
                    child_count = int(parts[4])
                    child_parts = parts[5].split(";", child_count)

                    max_timestamp = datetime.datetime.min
                    for child in child_parts:
                        timestamp = Event.parse_timestamp(child[1:child.index(",")])
                        if timestamp > max_timestamp:
                            max_timestamp = timestamp

                    latency = event.timestamp - max_timestamp
                    self.__latency.log_value(f"{event.event_type};{event.id};{child_count};{latency.total_seconds()}")
            elif line.startswith("complex") or line.startswith("simple"):
                # Received events
                # simple | eventID | timestamp | eventType
                # complex | eventID | timestamp [can be creationTime] | eventType | numberOfEvents | (individual Event);(individual Event)[;...]
                self.this_node.report_event(Event.from_flink(line, False))
                self.__event_count.inc_received()
            elif line.startswith("[Forward]"):
                start = line.index("->") + 4
                dest_count = line.count(",", start, -1) + 1
                self.__event_count.sent.value += dest_count
            elif line.startswith("[Selectivity]"):
                self.__discarded_count.inc()
            elif line.startswith("[EndOfStream]"):
                self.done_handler()

        print("[OutputReader] Done.")
