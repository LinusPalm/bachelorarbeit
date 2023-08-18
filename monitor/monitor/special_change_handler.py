import itertools
import logging
import socket
from typing import List, Optional, Tuple
from typing_extensions import Literal

from control_message import ControlMessage, ControlMessageService, TopologyConfigNode
from node_monitor import NodeMonitor
from plan_parser import EvaluationPlan, ForwardRule, Projection, ProjectionPart
from metrics import Metrics


class SpecialChangeHandler():
    control_socket: Optional[socket.socket] = None

    def __init__(self, control_message_service: ControlMessageService, monitor: NodeMonitor, plan: EvaluationPlan, topology_config: TopologyConfigNode, metrics: Metrics, ignore_change=False) -> None:
        self.plan = plan
        self.control_messages = control_message_service
        self.monitor = monitor
        self.ignore_change = ignore_change
        self.topology = topology_config
        self.prev_changes_received = set()
        self.prev_changes_sent = set()

        self.__command_count = metrics.get_counter("commands.sent")
        self.__harmful_count = metrics.get_traffic_counter("harmful_projections")

        if not ignore_change:
            monitor.add_change_handler(self.trigger_harmful_projection)
            #control_message_service.add_receiver("change_inputs", self.handle_harmful_projection) # type: ignore
            control_message_service.add_receiver("change_inputs", self.handle_stuff)
        else:
            logging.warn("Change handler disabled. Ignoring event rate changes.")

    def trigger_harmful_projection(self, node_id: str, projection: Projection):
        logging.debug(f"[ChangeHandler] Before change: {self.plan.to_json_str()}")

        keep_mode = "keep" if projection.is_used else "disable"

        affected_nodes = self.plan.forward_inputs(self.plan.nodes[node_id], projection, node_id)

        logging.info(f"[ChangeHandler] Repairing local harmful projection \"{projection.value} FROM {','.join([p.value for p in projection.parts])}\", Mode: {keep_mode}, Affected: {len(affected_nodes)}")

        self.send_forward_inputs_command(projection.value, keep_mode)

        for other_node in affected_nodes:
            params = [projection.value] + [p.value for p in projection.parts]
            signature = (other_node, *params)
            if not signature in self.prev_changes_sent:
                self.prev_changes_sent.add(signature)
                control_message = ControlMessage("change_inputs", params, node_id, other_node)
                self.control_messages.send_control_message(control_message)
            else:
                logging.info(f"[ChangeHandler] Not sending duplicate change_inputs \"{projection.value}\" to {other_node}")

        self.__harmful_count.inc_sent()

        # Flink implementation only allows one change per projection
        print("[Monitor] Stopped monitoring", projection.value)
        projection.is_forwarded = False
        if keep_mode == "disable":
            proj_list = self.plan.nodes[self.monitor.nodeId].projections[projection.value]
            proj_list.remove(projection)
            if len(proj_list) == 0:
                self.plan.nodes[self.monitor.nodeId].projections.pop(projection.value)

        logging.debug(f"[ChangeHandler] After change: {self.plan.to_json_str()}")

    def handle_stuff(self, message: ControlMessage):
        """This node will no longer receive matches of the harmful projection p from source node A. It will instead receive the inputs of p."""

        self.__harmful_count.inc_received()

        signature = (message.sourceNode, *message.params)
        if signature in self.prev_changes_received:
            logging.info(f"[ChangeHandler] Ignoring duplicate harmful projection \"{message.params[0]}\" from {message.sourceNode}")
            return
        
        self.prev_changes_received.add(signature)
        logging.debug(f"[ChangeHandler] Before change: {self.plan.to_json_str()}")

        harmful_projection = message.params[0] # Harmful projection p
        proj_replacements = sorted(message.params[1:])
        source_node_id = str(message.forwardNode)
        this_node = self.plan.nodes[self.monitor.nodeId]

        new_projections = {}

        # Check if there are any projections q that use p as an input
        changed_projections: List[Tuple[Projection, bool]] = []

        same_projections = [ p for p in this_node.projections.get(harmful_projection, []) if sorted([ pv.value for pv in p.parts ]) == proj_replacements ]
 
        has_same_projection = len(same_projections) > 0
        for same_proj in same_projections:
            for part in same_proj.parts:
                part.sources[source_node_id] = 1.0

        replacement_parts: List[ProjectionPart] = []
        for harmful_part in proj_replacements:
            new_sources = { source_node_id: 1.0 }
            if this_node.eventRates.get(harmful_part, 0) > 0 or harmful_part in this_node.projections:
                new_sources[this_node.nodeId] = 1
            
            replacement_parts.append(ProjectionPart(harmful_part, new_sources))

        def remove_source_from_part(proj: Projection, part: ProjectionPart, source_id: str):
            if source_id in part.sources:
                part.sources.pop(source_id)
            else:
                logging.error(f"Source node id {source_id} not in part sources of {part} of projection '{proj.value} from {[ p.value for p in proj.parts]}'.\nMessage: {message.params} from {message.sourceNode} via {message.forwardNode}, Has same: {has_same_projection}")

        for projection in itertools.chain.from_iterable(this_node.projections.values()):
            affected_parts = [ p for p in projection.parts if harmful_projection == p ]
            for part in affected_parts:
                if has_same_projection:
                    remove_source_from_part(projection, part, source_node_id)
                else:
                    if len(part.sources) == 1 and not any(p for p in self.plan.nodes[source_node_id].projections.get(part.value, []) if sorted([ pv.value for pv in p.parts ]) != proj_replacements):
                        # If A is the only source for p used by q, replace q with q' where p has been replaced by the inputs of p
                        projection.parts.remove(part)
                        projection.parts += replacement_parts
                        changed_projections.append((projection, True))
                    else:
                        # If A is not the only source for p used by q, add a new projection q' where p has been replaced by the inputs of p
                        if len(part.sources) > 1:
                            remove_source_from_part(projection, part, source_node_id)

                        proj_parts = replacement_parts + [ ProjectionPart(p.value, p.sources.copy()) for p in projection.parts if p != part ]
                        new_proj = Projection(projection.value, projection.selectivity, proj_parts, projection.is_used, projection.is_forwarded)
                        new_projections[projection.value] = this_node.projections[projection.value] + [ new_proj ]                  

                        changed_projections.append((projection, False))
        
        this_node.projections = { **this_node.projections, **new_projections }

        affected_rule = next((r for r in this_node.forwardRules if r.projection == harmful_projection and r.fromNode == source_node_id ), None)
        rule_changes = 0
        if affected_rule != None:
            # If this node has a projection p' of the same type as the harmful projection p,
            # it will be used to match the harmful projection now.
            # Inputs of p will not be forwarded further. This means that no further changes are necessary on other nodes.
            # It also reduces data transmissions, due to selectivity of p'.
            if not has_same_projection:
                new_rules = []

                def add_rule_for_source(part: ProjectionPart, source: str):
                    nonlocal rule_changes
                    rule_changes += 1
                    existing_rules = [r for r in this_node.forwardRules if r.projection == part.value and r.fromNode == source]
                    if not existing_rules:
                        new_rules.append(ForwardRule(part.value, source, affected_rule.toNodes))
                    else:
                        for existing in existing_rules:
                            existing.toNodes = list(set(existing.toNodes).union(affected_rule.toNodes))

                for part in replacement_parts:
                    for source in part.sources.keys():
                        add_rule_for_source(part, source)

                    add_rule_for_source(part, source_node_id)

                this_node.forwardRules += new_rules

                for other_node in affected_rule.toNodes:
                    signature = ("msg", other_node, *message.params)
                    if signature not in self.prev_changes_sent:
                        self.prev_changes_sent.add(signature)
                        new_message = ControlMessage("change_inputs", message.params, this_node.nodeId, other_node)
                        self.control_messages.send_control_message(new_message)
                    else:
                        logging.info(f"[ChangeHandler] Not sending duplicate change_inputs \"{message.params[0]}\" to {other_node}")

                if len(affected_rule.toNodes) > 0:
                    self.send_change_rules_command(harmful_projection, source_node_id, "projection", proj_replacements)
            else:
                duplicate_rule = next((r for r in this_node.forwardRules if r.fromNode == this_node.nodeId and r.projection == harmful_projection), None)
                if duplicate_rule == None:
                    this_node.forwardRules.append(ForwardRule(harmful_projection, this_node.nodeId, affected_rule.toNodes))
                else:
                    duplicate_rule.toNodes = list(set(duplicate_rule.toNodes).union(affected_rule.toNodes))

                self.send_change_rules_command(harmful_projection, source_node_id, "source", [ this_node.nodeId ])


            this_node.forwardRules.remove(affected_rule)

        changed_count = 0
        for changed_proj, was_removed in changed_projections:
            # Flink implementation only allows one change per projection
            changed_proj.is_used = was_removed
            changed_proj.is_forwarded = False

            keep_mode = "disable" if was_removed else "keep"
            signature = ("cmd", harmful_projection, changed_proj.value, keep_mode)
            if signature not in self.prev_changes_sent:
                self.send_switch_pattern_command(harmful_projection, changed_proj.value, keep_mode)
                self.prev_changes_sent.add(signature)
                changed_count += 1
            else:
                logging.debug(f"[ChangeHandler] Ignoring duplicate switch_pattern {harmful_projection} {changed_proj.value} {keep_mode}")

        logging.info(f"[ChangeHandler] Handled harmful projection \"{message.params[0]}\" from {message.sourceNode}. Proj changes: {changed_count}, Has same: {has_same_projection}, New rules: {rule_changes}")
        logging.debug(f"[ChangeHandler] After change: {self.plan.to_json_str()}")


    def send_change_rules_command(self, projection: str, from_node_id: str, replace_mode: Literal["projection", "source"], replacements: List[str]):
        self.send_flink_control_message("change_rules", [ projection, from_node_id, replace_mode ] + replacements)

    def send_forward_inputs_command(self, projection: str, keep_mode: Literal["keep", "disable"]):
        self.send_flink_control_message("forward_inputs", [projection, keep_mode])

    def send_switch_pattern_command(self, trigger_input: str, projection: str, keep_mode: Literal["keep", "disable"]):
        self.send_flink_control_message("switch_pattern", [trigger_input, projection, keep_mode])

    def send_flink_control_message(self, type: Literal["forward_inputs", "switch_pattern", "change_rules"], parameters: List[str]):
        if self.control_socket == None:
            self.control_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.control_socket.connect((self.topology.ip_address, self.topology.port))
            hello = f"hello | {self.monitor.nodeId}\n"
            self.control_socket.sendall(hello.encode())
            logging.debug("Engine command socket connected")

        params = " | ".join(parameters)
        message = f"control | {type} | {params}"

        logging.debug(f"Sending engine command \"{message}\"")

        message = (message + "\n").encode()

        self.control_socket.sendall(message)
        self.__command_count.inc()
    
    def unregister(self):
        if not self.ignore_change:
            self.monitor.remove_change_handler(self.trigger_harmful_projection)
            self.control_messages.remove_receiver("change_inputs", self.handle_stuff)

    def close(self):
        self.unregister()

        if self.control_socket != None:
            self.control_socket.close()
