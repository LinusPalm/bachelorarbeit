from argparse import ArgumentParser
import datetime
from os import path
import socket
import sys
import threading
import time
from typing import Optional
import uuid
from control_message import TopologyConfig, TopologyConfigNode

def parse_child_event(value: str):
    parts = value.strip("()").split(";")
    if len(parts) == 2:
        event_id = str(uuid.uuid4())[0:8]
    else:
        event_id = parts[2]

    return "(" + ",".join([parts[0], event_id, parts[1]]) + ")"

def parse_child_event_dated(value: str, now: datetime.datetime):
    parts = value.strip("()").split(";")
    if len(parts) == 2:
        event_id = str(uuid.uuid4())[0:8]
    else:
        event_id = parts[2]

    return "(" + ",".join([format_timestamp(parts[0], now), event_id, parts[1]]) + ")"

def parse_timestamp(timestamp: str):
    parts = timestamp.split(":")
    
    if len(parts) == 4:
        milliseconds = int(parts[3])
    else:
        milliseconds = 0

    return datetime.timedelta(hours=int(parts[0]), minutes=int(parts[1]), seconds=int(parts[2]), milliseconds=milliseconds)

def format_timestamp(timestamp: str, now: datetime.datetime):
    parsed = parse_timestamp(timestamp)
    date = now + parsed

    return date.isoformat(timespec="milliseconds") + "Z"

def read_and_send_event_stream(config: TopologyConfigNode, input_file: str, cancel_event: threading.Event, keepOpen: bool = False, useDates: bool = False):
    print(f"[send_eventstream] Sending events from \"{input_file}\" to {config.ip_address}:{config.port}\n")

    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    attempts = 1
    while True:
        try:
            client_socket.connect((config.ip_address, config.port))
            break
        except ConnectionRefusedError as e:
            sys.stderr.write(f"[send_eventstream] Error while connecting: {e}\n")
            attempts += 1
            if attempts > 3:
                sys.stderr.write("[send_eventstream] Max connection attempts reached. Aborting.")
                return

            time.sleep(5)

    hello = f"hello | {config.id}\n"
    client_socket.send(hello.encode())

    now = datetime.datetime.utcnow()
    prevEvent = ""
    prevTime: Optional[datetime.timedelta] = None
    prevEventCount = 0

    with open(input_file, "r") as f:
        line = f.readline()
        while not cancel_event.is_set() and line != None and line != "":
            attributes = line.rstrip("\n").split(",")

            timestampRaw = attributes[0]

            if timestampRaw == prevEvent:
                prevEventCount += 1
            else:
                prevEvent = timestampRaw
                prevEventCount = 0

            timestampRaw += ":" + str(prevEventCount)

            if useDates:
                timestamp = format_timestamp(timestampRaw, now)
            else:
                timestamp = timestampRaw

            event_type = attributes[1]

            if len(attributes) > 2 and ";" not in event_type:
                event_id = attributes[2]
            else:
                event_id = str(uuid.uuid4())[0:8]

            if ";" in event_type:
                if useDates:
                    child_events = [ parse_child_event_dated(e, now) for e in attributes[2:] ]
                else:
                    child_events = [ parse_child_event(e) for e in attributes[2:] ]

                parts = [ "complex", event_id, timestamp, event_type.replace(";", ","), str(len(child_events)), ";".join(child_events) ]
            else:
                parts = [ "simple", event_id, timestamp, event_type ]

            message = " | ".join(parts) + "\n"

            currentTime = parse_timestamp(timestampRaw)
            if prevTime != None:
                delay = (currentTime - prevTime).total_seconds()
                time.sleep(delay)

            prevTime = currentTime

            try:
                client_socket.send(message.encode())
            except Exception as error:
                print(f"Error while sending \"{message}\":", error)

            line = f.readline()

    if not keepOpen:
        end_message = "end-of-the-stream\n".encode()

        try:
            client_socket.send(end_message)
        except OSError:
            print("Unable to send end-of-the-stream message")

    client_socket.close()
    print("[send_eventstream] Done.")


def main():
    parser = ArgumentParser()
    parser.add_argument("configFile", help="Path to json network config file.")
    parser.add_argument("-i", "--input", help="Event input file to use instead of file specified in node config.")
    parser.add_argument("-n", "--node", help="Node id override.")
    parser.add_argument("--keepOpen", action="store_true", help="Do not send end-of-the-stream message.", default=False)
    parser.add_argument("--useDates", action="store_true", help="Add current date to all timestamps.", default=False)

    args = parser.parse_args()

    configFileName: str = args.configFile
    if not path.isfile(configFileName):
        sys.stderr.write("Config file does not exist.\n")
        exit(-1)

    config = TopologyConfig.parse(configFileName)

    if not args.node:
        # setup socket and get own ip
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.connect(("8.8.8.8", 80))
            own_ip = sock.getsockname()[0]
            print("[send_eventstream] Own ip: ", own_ip)

        ownConfig = next((n for n in config.nodes.values() if n.ip_address == own_ip), None)
        if ownConfig == None:
            sys.stderr.write(f"Could not find config for \"{own_ip}\".\n")
            exit(-1)
    else:
        ownConfig = config.nodes[args.node]

    inputFile = args.input

    cancel = threading.Event()

    try:
        read_and_send_event_stream(ownConfig, inputFile, cancel, args.keepOpen, args.useDates)
    except KeyboardInterrupt:
        cancel.set()

if __name__ == "__main__":
    main()
