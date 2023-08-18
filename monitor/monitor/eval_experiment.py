import argparse
import datetime
from json import JSONDecodeError, loads
import math
from numbers import Number
import pathlib
import sys
import os
import traceback
from typing import List, Tuple, Union

import numpy as np
from plan_parser import EvaluationPlan
from control_message import TopologyConfig
from calc_central_costs import calc_centralized_cost

def get_total_sent(metrics: dict, no_commands: bool):
    control_msg_count = metrics["control_message.sent"] - metrics["engine_ready.sent"] - metrics["engine_done.sent"]
    monitor_msg_count = control_msg_count
    command_count = metrics.get("commands.sent", 0)
    if not no_commands:
        monitor_msg_count += command_count
    else:
        if command_count > 0:
            print(f"Ignoring {command_count} commands")

    node_sent = metrics["events.sent"] + monitor_msg_count

    return node_sent, monitor_msg_count

def read_metrics(metric_dir: str, node_count: int, use_clean: bool, no_commands: bool):
    sent_events = 0
    changes = 0
    changes_handled = 0
    almost = 0

    for node in range(node_count):
        clean_file = os.path.join(metric_dir, f"clean_metrics_{node}.csv")
        if use_clean and os.path.isfile(clean_file):
            metric_file = clean_file
            print("Using clean metrics for node", node)
        else:
            metric_file = os.path.join(metric_dir, f"metrics_{node}.csv")

        node_metrics = {}
        with open(metric_file, "r") as file:
            lines = file.readlines()
            for line in lines[1:]:
                key, value = line.strip("\n").split(",", 2)

                if key.endswith(".sent") or key.endswith(".received") or key == "almost_threshold":
                    value = int(value)

                node_metrics[key] = value
    
        # events.sent: Events that the flink engine forwarded to other nodes
        # control_message.sent: Control messages that the monitor sent/forwarded to other node monitors
        # commands.sent: Engine commands sent to the flink engine by the node monitor
        # engine_ready.sent, engine_done.sent: Part of control_message.sent, but only used for experiment orchestration, thus ignored in statistics

        node_sent, monitor_msg_count = get_total_sent(node_metrics, no_commands)
        
        sent_events += node_sent
        changes += node_metrics["harmful_projections.sent"]
        changes_handled += node_metrics["harmful_projections.received"]
        almost += node_metrics["almost_threshold"]

        if node_metrics["almost_threshold"] > 0:
            print(f"{metric_dir}: Node {node} sent {node_sent} events (by monitor: {monitor_msg_count}). Harmful projections: (Triggered: {node_metrics['harmful_projections.sent']}, Handled:  {node_metrics['harmful_projections.received']}, Almost: {node_metrics['almost_threshold']})")

    return (sent_events, changes, changes_handled, almost)


def read_periodic_metrics(metric_dir: pathlib.Path, node_count: int, use_clean: bool):
    period_totals = {}
    for node_id in range(node_count):
        clean_file = metric_dir.joinpath(f"clean_periodic_{node_id}.txt")
        if use_clean and clean_file.is_file():
            periodic_metric_file = clean_file
        else:
            periodic_metric_file = metric_dir.joinpath(f"periodic_{node_id}.txt")

            if not periodic_metric_file.is_file():
                return {}
        
        with periodic_metric_file.open("r") as file:
            lines = file.readlines()

        period = 0
        for line in lines[2:]:
            if "datetime" in line:
                start = line.index("(")
                end = line.index(")", start)

                ts_parts = [ int(p) for p in line[start + 1:end].split(", ") ]
                ts = datetime.datetime(*ts_parts) # type: ignore
                line = line.replace(f"datetime.datetime{line[start:end]})", f"'{ts}'")

            snapshot: dict = loads(line.replace("'", '"'))

            if period not in period_totals:
                period_totals[period] = {}

            for key, value in snapshot.items():
                if isinstance(value, Number):
                    if key not in period_totals[period]:
                        period_totals[period][key] = 0
                    
                    period_totals[period][key] += value
                elif key == "ts":
                    if key not in period_totals[period]:
                        period_totals[period][key] = 0

                    period_totals[period][key] += datetime.datetime.fromisoformat(value).timestamp()

            period += 1

    for period in period_totals.values():
        period["ts"] = datetime.datetime.fromtimestamp(period["ts"] / node_count)
    
    return period_totals


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("plan", help="Plan directory")
    parser.add_argument("metrics", help="Metrics directory")
    parser.add_argument("--control", "-c", help="Control group metrics directory")
    parser.add_argument("--oneplan", action="store_true", help="Only one plan per experiment.")
    parser.add_argument("--output", default=None, help="Optional output file path.")
    parser.add_argument("--use-clean", dest="use_clean", action="store_true", help="Use clean_metrics_[nodeId].csv files if available")
    parser.add_argument("--no-commands", dest="no_commands", action="store_true", help="Do not include sent engine commands in sent metric.")
    parser.add_argument("--periodic", action="store_true", help="Evaluate periodic metric files")

    args = parser.parse_args()
    no_commands: bool = args.no_commands
    use_clean: bool = args.use_clean

    plan_dir = pathlib.Path(args.plan)
    metric_dir = pathlib.Path(args.metrics)
    if "control" in args and args.control:
        control_metric_dir = pathlib.Path(args.control)
    else:
        control_metric_dir = None

    """if not os.path.isfile(plan_file):
        sys.stderr.write("Plan file not found.\n")
        exit(1)"""

    if not os.path.isdir(metric_dir):
        sys.stderr.write("Metrics dir not found.\n")
        exit(1)

    if control_metric_dir and not os.path.isdir(control_metric_dir):
        sys.stderr.write("Control group metrics dir not found.\n")
        exit(1)

    """if plan_file.endswith('.json'):
        plan = EvaluationPlan.parse_from_flink(plan_file)
    else:
        plan = EvaluationPlan.parse(plan_file)"""

    """node_count = len(plan.nodes)"""

    values: List[Tuple[str, Union[int, str], int, float, str, List[tuple], float, float, List[tuple], float, float, List[Tuple[str, int, int, float, int, int, int]]]] = []
    max_runs = 0
    max_control_runs = 0

    central_plan_nodes = {}

    """for experiment in metric_dir.iterdir():
        if not experiment.is_dir():
            continue

        for run in experiment.glob("run_*"):
            read_metrics(run.as_posix(), len(list(run.glob("metrics_*.csv"))))"""

    for experiment in metric_dir.iterdir():
        if not experiment.is_dir():
            continue

        id = experiment.name if "control" not in experiment.name else "swap_8_4"
        first = id.index("_") + 1
        second = id[first:].index("_")
        plan_id = id[first:first + second]
        mod_id = id[first + second + 1:]
   
        plan_file = plan_dir.joinpath(plan_id + "/")
        if mod_id == "0" or args.oneplan:
            plan_file = next(plan_file.glob("*_MS.txt"))
        else:
            mod = id[:first - 1]
            mod = "min"
            print(f"Plan: {plan_file}, Mod: {mod}, Mod Id: {mod_id}")
            plan_file = next(plan_file.glob(f"*_MS_{mod}{mod_id}.txt"))

        if not plan_file.is_file():
            print("Plan file", plan_file.as_posix(), "not found")
            exit(1)

        topology = TopologyConfig.parse(plan_file.parent.joinpath("network_local.json").as_posix())

        plan = EvaluationPlan.parse(plan_file.as_posix())
        node_count = len(plan.nodes)
        centralized_cost, central_node = calc_centralized_cost(plan, topology, central_node=central_plan_nodes.get(plan_id, None), with_central_node=True) # type: ignore
        if plan_id not in central_plan_nodes:
            central_plan_nodes[plan_id] = central_node

        def get_averages(all_trs):
            return (sum(all_trs) / len(all_trs), np.median(all_trs))

        runs = []
        periods = []
        for run in sorted(list(experiment.glob("run_*"))):
            if args.periodic:
                periodic_run = read_periodic_metrics(run, node_count, use_clean)
                if len(periodic_run) > 0:
                    first_period = periodic_run[next(iter(periodic_run.keys()))]
                    if "ts" not in first_period:
                        print(run, periodic_run)

                    start_ts = first_period["ts"]
                    for key, period in periodic_run.items():
                        period_sent, period_controls = get_total_sent(period, no_commands)
                        period_tr = period_sent / centralized_cost
                        periods.append((
                            run.name[run.name.index("_") + 1:],
                            key,
                            (period["ts"] - start_ts).total_seconds(),
                            period_sent,
                            period["events.received"],
                            period_tr, 
                            period["harmful_projections.sent"],
                            period["harmful_projections.received"],
                            period["almost_threshold"]))

            sent, changes, changes_handled, almost = read_metrics(run.as_posix(), node_count, use_clean, no_commands)
            tr_ratio = sent / centralized_cost
            runs.append((sent, tr_ratio, changes, changes_handled, almost))

        if len(runs) > max_runs:
            max_runs = len(runs)
        
        avg_tr, median_tr = get_averages([ x[1] for x in runs ])

        control_runs = []
        if control_metric_dir != None:
            for control_run in sorted(list(control_metric_dir.joinpath(id).glob("run_*"))):
                sent, _, _, _ = read_metrics(control_run.as_posix(), node_count, True, no_commands)
                tr_ratio = sent / centralized_cost
                control_runs.append((sent, tr_ratio))

            if len(control_runs) > max_control_runs:
                max_control_runs = len(control_runs)

            if len(control_runs) > 0:
                avg_control_tr, median_control_tr = get_averages([ x[1] for x in control_runs ])
            else:
                avg_control_tr = 0
                median_control_tr = 0
        else:
            avg_control_tr = 0
            median_control_tr = 0

        try:
            plan_id = int(plan_id)
        except ValueError:
            plan_id = 157

        values.append((experiment.name, plan_id, int(mod_id), centralized_cost, central_node, runs, avg_tr, median_tr, control_runs, avg_control_tr, median_control_tr, periods))

    values = sorted(values, key=lambda x: (x[1], x[2]))
    if args.output != None and args.output.strip() != "":
        results_file = open(args.output, "w")
        output_path = args.output
    else:
        output_path = metric_dir.joinpath("results.csv")
        results_file = output_path.open("w")
        output_path = output_path.as_posix()
    
    if args.periodic:
        period_output = open(metric_dir.joinpath("periodic_results.csv"), "w")
        period_output.write("id,plan,mod,centralized_cost,central_node,run,period,ts,sent,received,tr,triggered,handled,almost\n")

    results_file.write("id,plan,mod,centralized_cost,central_node")
    for i in range(max_runs):
        results_file.write(f",sent_{i},tr_{i},triggered_{i},handled_{i},almost_{i}")

    if max_runs > 0:
        results_file.write(",avg_tr,median_tr")

    for i in range(max_control_runs):
        results_file.write(f",control_sent_{i},control_tr_{i}")

    if max_control_runs > 0:
        results_file.write(",avg_control_tr,median_control_tr")

    results_file.write("\n")
    for value in values:
        # Experiment id, plan, mod, centralized cost, central node
        results_file.write(f"{value[0]},{value[1]},{value[2]},{value[3]},{value[4]}")

        # Runs (sent, tr, triggered, handled)
        for run in value[5]:
            results_file.write(",")
            results_file.write(",".join([ str(x) for x in run]))
        
        for i in range(max_runs - len(value[5])):
            results_file.write(",,,,,")

        # Average tr, median tr
        results_file.write(f",{value[6]},{value[7]}")
        
        if max_control_runs > 0:
            # Control runs (sent, tr)
            for control in value[8]:
                results_file.write(",")
                results_file.write(",".join([ str(x) for x in control]))

            for i in range(max_control_runs - len(value[8])):
                results_file.write(",,")

            # Average control tr, median control tr
            results_file.write(f",{value[9]},{value[10]}")

        results_file.write("\n")

        if args.periodic:
            for period in value[11]:
                period_output.write(f"{value[0]},{value[1]},{value[2]},{value[3]},{value[4]},{period[0]},{period[1]},{period[2]},{period[3]},{period[4]},{period[5]},{period[6]},{period[7]},{period[8]}\n") #type: ignore

    results_file.close()
    print("Wrote results to", output_path)

    if args.periodic:
        period_output.close() # type: ignore


if __name__ == "__main__":
    main()
