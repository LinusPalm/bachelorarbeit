from dataclasses import dataclass
import datetime
import itertools
from queue import PriorityQueue
from typing import Callable, Dict, List, Optional
from metrics import Metrics
from plan_parser import EvaluationPlan, Projection
from threading import Lock


ChangeHandlerType = Callable[[str, Projection], None]

@dataclass
class Event:
    kind: str
    id: str
    event_type: str
    timestamp: datetime.datetime
    generated: bool

    def __lt__(self, other):
        return self.timestamp < other.timestamp
    
    def __gt__(self, other):
        return self.timestamp > other.timestamp

    @staticmethod
    def parse_timestamp(raw_value: str):
        # Python does not like the Z at the end
        return datetime.datetime.fromisoformat(raw_value[:-1])

    @staticmethod
    def from_flink(value: str, generated: bool):
        parts = value.split(" | ")
        return Event(parts[0], parts[1], parts[3], Event.parse_timestamp(parts[2]), generated)


class EventTimeSlidingWindow:
    total_generated: Dict[str, int]
    total_received: Dict[str, int]
    events: 'PriorityQueue[Event]'
    count: int

    def __init__(self, window_size: datetime.timedelta, slide_size: datetime.timedelta, handler: Callable[[Optional[Event]], None]) -> None:
        self.window_size = window_size
        self.slide_size = slide_size
        self.handler = handler

        self.events = PriorityQueue()
        self.count = 0
        self.total_received = {}
        self.total_generated = {}
        self.lock = Lock()
        self.current_start = datetime.datetime.utcnow()
        self.current_end = self.current_start + window_size

    def __log_window(self, discarded_count: int):
        windowLength = (self.current_end - self.current_start).total_seconds()
        if len(self.events.queue) > 1:
            actualLength = (self.events.queue[-1].timestamp - self.events.queue[0].timestamp).total_seconds()
        else:
            actualLength = 0

        if self.current_start.date() == self.current_end.date():
            startPrint = self.current_start.time()
            endPrint = self.current_end.time()
        else:
            startPrint = self.current_start
            endPrint = self.current_end

        print("New window from", startPrint, "to", endPrint, f"({windowLength} s, {actualLength:0} s)", discarded_count, "discarded")

    def __slide(self):
        self.current_start += self.slide_size
        self.current_end += self.slide_size

        discardedCount = 0

        while self.events.queue and self.events.queue[0].timestamp < self.current_start:
            oldEvent = self.events.get()
            if oldEvent.generated:
                self.total_generated[oldEvent.event_type] -= 1
                if self.total_generated[oldEvent.event_type] == 0:
                    del self.total_generated[oldEvent.event_type]
            else:
                self.total_received[oldEvent.event_type] -= 1
                if self.total_received[oldEvent.event_type] == 0:
                    del self.total_received[oldEvent.event_type]
            
            self.count -= 1
            discardedCount += 1
        
        return discardedCount
        

    def add_event(self, event: Event):
        self.lock.acquire()

        if event.timestamp < self.current_start:
            print("WARNING: Out of order event:", event.id)

        while event.timestamp > self.current_end:
            self.handler(None)

            current_discarded = self.__slide()
            self.__log_window(current_discarded)

        if event.generated:
            interval_dict = self.total_generated
        else:
            interval_dict = self.total_received

        if event.event_type in interval_dict:
            interval_dict[event.event_type] += 1
        else:
            interval_dict[event.event_type] = 1

        self.events.put(event)
        self.count += 1
        
        self.lock.release()
    
    def __repr__(self) -> str:
        return f"EventTimeSlidingWindow(window_size={self.window_size}, slide_size={self.slide_size}, start={self.current_start}, end={self.current_end}, event_count={len(self.events.queue)})"

class NodeMonitor():
    currentReceived: Dict[str, int]
    currentGenerated: Dict[str, int]

    __changeHandlers: List[ChangeHandlerType] = []

    def __init__(self, nodeId: str, plan: EvaluationPlan, metrics: Metrics, window_size=60, slide_size=1, threshold=10, log_slides=False) -> None:
        self.nodeId = nodeId
        self.plan = plan
        self.window_size = window_size
        self.slide_size = slide_size
        self.threshold = threshold
        self.log_slides = log_slides

        self.window = EventTimeSlidingWindow(
            window_size=datetime.timedelta(seconds=window_size),
            slide_size=datetime.timedelta(seconds=slide_size),
            handler=self.check_sliding_window)

        self.currentReceived = {}
        self.currentGenerated = {}

        self.__changeHandlers = []
        self.__slide_count = metrics.get_counter("window_slides")
        self.__almost_threshold = metrics.get_counter("almost_threshold")
        if log_slides:
            self.__interval_log = metrics.get_value_history(f"intervals_{nodeId}")

        self.lock = Lock()

        print("[Monitor] Projections to be monitored:")
        monitor_any = False
        for proj in itertools.chain.from_iterable(self.plan.nodes[nodeId].projections.values()):
            if proj.is_forwarded:
                print(" -", proj.value, "FROM", [ p.value for p in proj.parts ])
                monitor_any = True

        if not monitor_any:
            print("-- No projections will be monitored! --")
        
        print()

    def log(self, *values):
        print("[Monitor]", *values)

    def check_sliding_window(self, new_event: Optional[Event]):
        generated = self.window.total_generated
        received = self.window.total_received
        self.__slide_count.inc()
        if self.log_slides:
            self.__interval_log.log_value({ "rec": received, "gen": generated })


        if self.window.count == 0:
            return

        # Plan is modified if change is detected, so we need to iterate over copy 
        monitored_projs = [ p for p in itertools.chain.from_iterable(self.plan.nodes[self.nodeId].projections.values()) if p.is_forwarded ]

        for projection in monitored_projs:
            inputSum = 0
            for part in projection.parts:
                partReceived = received.get(part.value, 0)
                partGenerated = generated.get(part.value, 0)

                # Use both generated and received rates, 
                # as local matches could be used as input in another local projection
                inputSum += partReceived + partGenerated

            # INEv 6.6: "A projection becomes harmful, if its output rate becomes higher than the sum of its input rates."
            countedRate = generated.get(projection.value, 0)
            countedDiff = countedRate - inputSum
            if countedDiff > self.threshold:
                self.__on_change(projection)
                self.log(f"{projection.value}: Observed Output Rate ({countedRate}) > Input Sum ({inputSum})")
            elif countedDiff > 0:
                self.__almost_threshold.inc()
                self.log(f"{projection.value}: Different, but under threshold, Observed Output Rate ({countedRate}) > Input Sum ({inputSum})")

            self.log(f"[{projection.value}] Input: {inputSum}, Out (Actual): {countedRate}")

    def report_event(self, event: Event):
        """
        Adds event to the current window interval. 
        Set generated to True, if the event was matched by the node of this monitor.
        """
        self.window.add_event(event)

    def add_change_handler(self, handler: ChangeHandlerType):
        """
        Add a function that is called when the node monitor detects a change in event rates.
        """
        self.__changeHandlers.append(handler)

    def remove_change_handler(self, handler: ChangeHandlerType):
        """
        Remove a previously added change handler function.
        """
        self.__changeHandlers.append(handler)

    def __on_change(self, projection: Projection):
        for handler in self.__changeHandlers:
            handler(self.nodeId, projection)
