import datetime
import logging
import subprocess
from os import mkdir, path
import sys
import threading
import signal
import traceback

from control_message import ControlMessage, ControlMessageService, TopologyConfig
from config import EngineProfile, MonitorConfig
from metrics import Metrics
from special_change_handler import SpecialChangeHandler
from utils import IntervalTimer
from node_monitor import NodeMonitor
from output_reader import FlinkEngineOutputReader
from plan_parser import EvaluationPlan
import send_eventstream

def main():
    parser = MonitorConfig()
    parser.add_argument("--engine", required=True, argOnly=True, help="Path to engine profile json file.")
    parser.add_argument("-p", "--plan", required=True, help="Path to evaluation plan file.")
    parser.add_argument("-c", "--config", required=True, help="Path to network config.")
    parser.add_argument("-n", "--node", required=True, help="Id of node to monitor.")
    parser.add_argument("--done-wait-time", dest="done_wait_time", default=120, help="Time in seconds to wait before shutdown after all nodes reported as done.")

    # Logging settings
    parser.add_argument("--passthrough", action="store_true", help="Log unused engine output.")
    parser.add_argument("--logdir", default="logs", help="Directory for log files.")
    parser.add_argument("--loglevel", default="INFO", help="Log level")
    parser.add_argument("--runid", default=None, help="Optional experiment run id.")
    parser.add_argument("--metric-interval", default=0, dest="metric_interval", help="Interval to store metrics periodically.")
    parser.add_argument("--log-latency", default=False, action="store_true", dest="log_latency", help="Log complex match latency")
    parser.add_argument("--log-slides", default=False, action="store_true", dest="log_slides", help="Log sliding window contents")

    # Node monitor settings
    parser.add_argument("--threshold", default=10, help="Threshold for Output rate > Input sum condition")
    parser.add_argument("--window-size", default=60, dest="window_size", help="Sliding window size in seconds.")
    parser.add_argument("--window-slide", default=1, dest="slide_size", help="Sliding window slide interval in seconds.")
    parser.add_argument("--ignore-change", action="store_true", dest="ignore_change", help="Do not send out control messages, if rate changes occur.")

    # send_eventstream settings
    parser.add_argument("--no-send", default=False, dest="no_send", action="store_true", help="Do not start send_eventstream for this node.")
    parser.add_argument("--inputFile", help="Event input file to use with send_eventstream. Takes priority over setting from network config.")

    args = parser.parse_args()

    logging.basicConfig(level=str.upper(args["loglevel"]), stream=sys.stdout)
    logging.info("Starting up...")
    if "runid" in args and args["runid"] != None:
        run_id = args["runid"]
        logging.info(f"Run id: {run_id}")

    if "inputFile" in args and args["inputFile"] != None:
        inputFile = args["inputFile"]
        if not path.isfile(inputFile):
            sys.stderr.write(f"Input file \"{inputFile}\" does not exist.")
            exit(-1)
    else:
        inputFile = None

    planFile: str = args["plan"]
    if not path.isfile(planFile):
        sys.stderr.write(f"Plan file \"{planFile}\" does not exist.\n")
        exit(-1)

    networkConfigFile: str = args["config"]
    if not path.isfile(networkConfigFile):
        sys.stderr.write(f"Network config file \"{networkConfigFile}\" does not exist.\n")
        exit(-1)

    logDir: str = args["logdir"]
    if not path.isdir(logDir):
        mkdir(logDir)
        logging.debug(f"Created log dir: {logDir}")

    noSend: bool = args["no_send"]

    plan = EvaluationPlan.parse_from_flink(planFile)
    networkConfig = TopologyConfig.parse(networkConfigFile, id_filter=set(plan.nodes.keys()))

    logging.debug(f"Network config: {networkConfig.__dict__}")

    nodeId = args["node"]
    ready_nodes = set()
    ready_event = threading.Event()
    ready_lock = threading.Lock()

    def report_ready(msg: ControlMessage):
        ready_lock.acquire()

        if msg.sourceNode not in ready_nodes:
            ready_nodes.add(msg.sourceNode)
            print("Node", msg.sourceNode, "is ready")

            if len(ready_nodes) == len(plan.nodes):
                ready_event.set()

        ready_lock.release()

    done_nodes = set()
    cancel_event = threading.Event()

    def report_done(msg: ControlMessage):
        ready_lock.acquire()

        try:
            if msg.sourceNode not in done_nodes:
                done_nodes.add(msg.sourceNode)
                print("Node", msg.sourceNode, "is done")

                if len(done_nodes) == len(plan.nodes):
                    def stop_self():
                        print("All nodes are done. Shutting down...")
                        shutdown_node(None, None)

                    t = threading.Timer(int(args["done_wait_time"]), stop_self)
                    t.start()
        finally:
            ready_lock.release()

    metrics = Metrics(logDir, [("run_id", args["runid"]), ("args", '"' + " ".join(sys.argv) + '"')])
    controlMessenger = ControlMessageService(nodeId, networkConfig, metrics)
    controlMessenger.add_receiver("engine_ready", report_ready)
    controlMessenger.add_receiver("engine_done", report_done)
    controlMessenger.start_listening(networkConfig.nodes[nodeId].control_port)

    def handle_self_done():
        done_msg = ControlMessage("engine_done", [], nodeId, "BROADCAST")
        controlMessenger.broadcast_message(done_msg)

        report_done(done_msg)

    monitor = NodeMonitor(
        nodeId,
        plan,
        metrics,
        int(args["window_size"]), 
        int(args["slide_size"]),
        int(args["threshold"]),
        log_slides=bool(args["log_slides"]))

    metric_interval = None
    if args["metric_interval"] != None and int(args["metric_interval"]) > 0:
        interval = int(args["metric_interval"])
        metric_interval = IntervalTimer(interval)
        history = metrics.get_value_history(f"periodic_{nodeId}")
        history.log_value(f"Node id: {nodeId}, metric interval: {interval}, Run Id: {args['runid']}\n")

        def write_periodically():
            current_val: dict = { name: value.value for name,value in metrics.counters.items() }
            current_val["ts"] = str(datetime.datetime.now())

            history.log_value(current_val)

        metric_interval.subscribe(write_periodically)

    profile: EngineProfile = args["engine_profile"]
    engine_args = [ profile.engine_program ] + profile.engine_args

    print("Engine args:", engine_args)

    engine = subprocess.Popen(
        args=engine_args,
        cwd=profile.engine_cwd,
        bufsize=0,
        text=True,
        stdout=subprocess.PIPE,
        stderr=sys.stderr)
    
    if engine.poll():
        sys.stderr.write(f"Failed to start engine. Engine exit code: {engine.returncode}\n")
        exit(2)
    
    outputReader = FlinkEngineOutputReader(
        engine.stdout,  # type: ignore
        [ monitor ],
        passthrough=args["passthrough"],
        cancel_event=cancel_event,
        metrics=metrics,
        done_handler=handle_self_done,
        log_latency=bool(args["log_latency"]))
    changeHandler = SpecialChangeHandler(controlMessenger, monitor, plan, networkConfig.nodes[nodeId], metrics, args["ignore_change"])

    was_shutdown = False
    def shutdown_node(signum, frame):
        nonlocal was_shutdown
        if was_shutdown:
            return
        
        was_shutdown = True
        cancel_event.set()
        if engine.poll() == None:
            # Engine is still running -> kill it
            engine.terminate()

            print("Stopped engine. Exit code:", engine.poll())

        controlMessenger.close()
        changeHandler.close()

        if metric_interval != None:
            write_periodically()
            metric_interval.cancel()

        metrics.to_file(f"metrics_{nodeId}.csv")

        print("Shutdown")

    signal.signal(signal.SIGINT, shutdown_node)

    try:
        print("Waiting for engine to get ready...")

        outputReader.read_metadata()

        if cancel_event.is_set():
            return

        ready_msg = controlMessenger.broadcast_ready()
        report_ready(ready_msg)

        print("Engine ready. Waiting for other nodes...")

        if not ready_event.wait(timeout=120) and not cancel_event.is_set():
            raise TimeoutError(f"Timed out while waiting for other nodes. Ready: {ready_nodes}")

        if cancel_event.is_set():
            return  

        print("All engines ready. Starting to monitor node.")

        if not noSend:
            nodeConfig = networkConfig.nodes[nodeId]

            sendThread = threading.Thread(target=send_eventstream.read_and_send_event_stream,
                                          args=(nodeConfig, inputFile, cancel_event, False, True))
            sendThread.start()

        if metric_interval != None:
            metric_interval.start()

        outputReader.read_event_stream()
    except Exception as e:
        print()
        print(f"Error of type {type(e).__name__}:", e)
        traceback.print_exc()
        
        if cancel_event:
            cancel_event.set()
    finally:
        shutdown_node(None, None)


if __name__ == "__main__":
    main()
