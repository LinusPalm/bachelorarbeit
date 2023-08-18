from os import path
from typing import Any, Dict, List, Tuple

class MetricCounter:
    def __init__(self, name: str) -> None:
        self.name = name
        self.value = 0

    def inc(self):
        self.value += 1

class TrafficCounter:
    def __init__(self, sent: MetricCounter, received: MetricCounter) -> None:
        self.sent = sent
        self.received = received

    def inc_sent(self):
        self.sent.inc()

    def inc_received(self):
        self.received.inc()

class ValueHistoryMetric:
    def __init__(self, name: str, file_name: str):
        self.name = name
        self.file_name = file_name
        self.file = open(file_name, "w", buffering=1)

    def log_value(self, value: Any):
        self.file.write(str(value) + "\n")

    def close(self):
        self.file.close()

class Metrics:
    def __init__(self, metrics_dir: str, header: List[Tuple[str, str]]) -> None:
        self.counters: Dict[str, MetricCounter] = {}
        self.value_histories: Dict[str, ValueHistoryMetric] = {}
        self.metrics_dir = metrics_dir
        self.header = header

    def get_traffic_counter(self, name: str):
        return TrafficCounter(self.get_counter(name + ".sent"), self.get_counter(name + ".received"))
    
    def get_counter(self, name: str):
        if name not in self.counters:
            self.counters[name] = MetricCounter(name)

        return self.counters[name]
    
    def get_value_history(self, name: str):
        if name not in self.value_histories:
            file_name = path.join(self.metrics_dir, name + ".txt")
            self.value_histories[name] = ValueHistoryMetric(name, file_name)

        return self.value_histories[name]

    def to_file(self, file_name: str):
        file_name = path.join(self.metrics_dir, file_name)
        with open(file_name, "w") as file:
            file.write("metric,value\n")
            if self.header != None:
                for k, v in self.header:
                    file.write(f"{k},{v}\n")

            for name, count in self.counters.items():
                file.write(f"{name},{count.value}\n")

        for history in self.value_histories.values():
            history.close()
