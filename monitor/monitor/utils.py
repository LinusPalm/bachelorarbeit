import threading
import time
from typing import Callable, List

class IntervalTimer:
    def __init__(self, interval: int) -> None:
        self.interval = interval
        self.stopEvent = threading.Event()
        self.handlers: List[Callable] = []
        self.is_running = False

    def __doInterval(self):
        nextTime = time.time() + self.interval
        while not self.stopEvent.wait(nextTime - time.time()):
            nextTime += self.interval

            for handler in self.handlers:
                handler()
        
        self.is_running = False

    def subscribe(self, handler: Callable):
        self.handlers.append(handler)

    def unsubscribe(self, handler: Callable):
        self.handlers.remove(handler)

    def start(self):
        if not self.is_running:
            thread = threading.Thread(target=self.__doInterval)
            thread.start()

            self.is_running = True

    def cancel(self):
        self.stopEvent.set()
