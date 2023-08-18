from copy import deepcopy
import itertools
import logging
from typing import Callable, Dict, List, Optional, Tuple
from typing_extensions import Literal

from pytest import MonkeyPatch
import pytest
from control_message import ControlMessage, TopologyConfigNode, ControlMessageReceiver
from metrics import MetricCounter, TrafficCounter
from plan_parser import EvaluationPlan, ForwardRule, PlanNode, Projection, ProjectionPart
from node_monitor import ChangeHandlerType
from special_change_handler import SpecialChangeHandler

logging.basicConfig(level=logging.DEBUG)

class MockControlMessenger():
    def __init__(self, send_control_message: Optional[Callable[[ControlMessage], None]] = None) -> None:
        self.receivers: Dict[str, List[ControlMessageReceiver]] = {}
        if send_control_message == None:
            def noop(msg: ControlMessage):
                pass

            send_control_message = noop

        self.send_control_message = send_control_message

    def add_receiver(self, message_type: str, handler: ControlMessageReceiver):
        if message_type not in self.receivers:
            self.receivers[message_type] = []

        self.receivers[message_type].append(handler)

    def on_received(self, message: ControlMessage):
        for handler in self.receivers[message.type]:
            handler(message)


class MockNodeMonitor():
    def __init__(self, node_id: str) -> None:
        self.nodeId = node_id
        self.handlers: List[ChangeHandlerType] = []

    def add_change_handler(self, handler: ChangeHandlerType):
        self.handlers.append(handler)

    def remove_change_handler(self, handler: ChangeHandlerType):
        self.handlers.remove(handler)

    def on_change(self, node_id: str, projection: Projection):
        for handler in self.handlers:
            handler(node_id, projection)

class MockMetrics():
    def get_counter(self, name: str):
        return MetricCounter(name)
    
    def get_traffic_counter(self, name: str):
        return TrafficCounter(MetricCounter(name + ".sent"), MetricCounter(name + ".received"))

def basic_test_setup_3(plan_0: EvaluationPlan, plan_1: EvaluationPlan, plan_2: EvaluationPlan):
    mock_monitor_0 = MockNodeMonitor("0")
    mock_monitor_1 = MockNodeMonitor("1")
    mock_monitor_2 = MockNodeMonitor("2")

    topology_node_0 = TopologyConfigNode("0", "localhost", 5000, 5001, {}, [])
    topology_node_1 = TopologyConfigNode("1", "localhost", 5002, 5003, {}, [])
    topology_node_2 = TopologyConfigNode("2", "localhost", 5004, 5005, {}, [])
    mock_metrics = MockMetrics()

    mock_control_messenger_2 = MockControlMessenger()
    def send_to_2(msg: ControlMessage):
        msg.forwardNode = "1"
        mock_control_messenger_2.on_received(msg)

    mock_control_messenger_1 = MockControlMessenger(send_to_2)
    def send_to_1(msg: ControlMessage):
        msg.forwardNode = "0"
        mock_control_messenger_1.on_received(msg)

    mock_control_messenger_0 = MockControlMessenger(send_to_1)
    
    change_handler_0 = SpecialChangeHandler(mock_control_messenger_0, mock_monitor_0, plan_0, topology_node_0, mock_metrics, False) # type: ignore
    change_handler_1 = SpecialChangeHandler(mock_control_messenger_1, mock_monitor_1, plan_1, topology_node_1, mock_metrics, False) # type: ignore
    change_handler_2 = SpecialChangeHandler(mock_control_messenger_2, mock_monitor_2, plan_2, topology_node_2, mock_metrics, False) # type: ignore

    return (change_handler_0, change_handler_1, change_handler_2, mock_monitor_0, mock_monitor_1, mock_monitor_2)

def basic_test_setup_2(plan_0: EvaluationPlan, plan_1: EvaluationPlan):
    mock_monitor_0 = MockNodeMonitor("0")
    mock_monitor_1 = MockNodeMonitor("1")
    topology_node_0 = TopologyConfigNode("0", "localhost", 5000, 5001, {}, [])
    topology_node_1 = TopologyConfigNode("1", "localhost", 5002, 5003, {}, [])
    mock_metrics = MockMetrics()

    mock_control_messenger_1 = MockControlMessenger()
    def send_to_1(msg: ControlMessage):
        msg.forwardNode = "0"
        mock_control_messenger_1.on_received(msg)

    mock_control_messenger_0 = MockControlMessenger(send_to_1)
    
    change_handler_0 = SpecialChangeHandler(mock_control_messenger_0, mock_monitor_0, plan_0, topology_node_0, mock_metrics, False) # type: ignore
    change_handler_1 = SpecialChangeHandler(mock_control_messenger_1, mock_monitor_1, plan_1, topology_node_1, mock_metrics, False) # type: ignore
    
    return (change_handler_0, change_handler_1, mock_monitor_0, mock_monitor_1)

def assert_projection(actual: Projection, expected: Tuple[str, List[Tuple[str, Dict[str, float]]]]):
    assert actual.value == expected[0]
    assert len(actual.parts) == len(expected[1])
    for actual_part, expected_part in zip(sorted(actual.parts, key=lambda i: i.value), sorted(expected[1], key=lambda i: i[0])):
        assert actual_part.value == expected_part[0]
        assert actual_part.sources == expected_part[1], f"Sources of {actual.value}, {actual_part.sources} != {expected_part[1]}"

def assert_forward_rule(actual: ForwardRule, expected_rule: str, expected_from: str, expected_to: List[str]):
    assert actual.projection == expected_rule
    assert actual.fromNode == expected_from
    assert sorted(actual.toNodes) == sorted(expected_to)


def test_forwarded_not_used_locally_used_on_dest_not_forwarded(monkeypatch: MonkeyPatch):
    and_a_b = Projection("AND(A,B)", 1, [ ProjectionPart("A", {}), ProjectionPart("B", {}) ], False, False)
    and_a_b_c = Projection("AND(A,B,C)", 1, [ ProjectionPart("AND(A,B)", {}), ProjectionPart("C", {}) ], False, False)
    
    plan_0 = EvaluationPlan([ { "A": 1, "B": 1, "C": 1 }, { "A": 0, "B": 0, "C": 1 } ], { 
        "0": PlanNode(
            nodeId="0", 
            forwardRules=[ ForwardRule("AND(A,B)", "0", [ "1" ]) ], 
            projections={ "AND(A,B)": [ and_a_b ] },
            eventRates={ "A": 1, "B": 1 }
        ),
        "1": PlanNode(
            nodeId="1",
            forwardRules=[],
            projections={ "AND(A,B,C)": [ and_a_b_c ] },
            eventRates={ "A": 0, "B": 0, "C": 1 }
        )
    })

    assert and_a_b.is_forwarded == True
    assert and_a_b_c.is_used == False

    plan_1 = deepcopy(plan_0)

    change_handler_0, change_handler_1, mock_monitor_0, _ = basic_test_setup_2(plan_0, plan_1) # type: ignore

    was_called = 0
    def mock_send_flink_command(type: Literal["forward_inputs", "switch_pattern", "change_rules"], parameters: List[str]):
        assert type == "forward_inputs"
        assert parameters == [ "AND(A,B)", "disable" ]
        nonlocal was_called
        was_called += 1

    was_called_1 = 0
    def mock_send_flink_command_1(type: Literal["forward_inputs", "switch_pattern", "change_rules"], parameters: List[str]):
        assert type == "switch_pattern"
        assert parameters == [ "AND(A,B)", "AND(A,B,C)", "disable" ]
        nonlocal was_called_1
        was_called_1 += 1

    monkeypatch.setattr(change_handler_0, "send_flink_control_message", mock_send_flink_command)
    monkeypatch.setattr(change_handler_1, "send_flink_control_message", mock_send_flink_command_1)

    mock_monitor_0.on_change("0", and_a_b)

    assert and_a_b.is_forwarded == False
    assert and_a_b.is_used == False
    
    assert_projection(and_a_b_c, ("AND(A,B,C)", [("A", { "0": 1 }), ("B", { "0": 1 }), ("C", { "1": 1 })]))

    receiver_proj = plan_1.nodes["1"].projections["AND(A,B,C)"]

    assert len(receiver_proj) == 1

    assert_projection(receiver_proj[0], ("AND(A,B,C)", [("A", { "0": 1 }), ("B", { "0": 1 }), ("C", { "1": 1 })]))

    forward_rules = plan_0.nodes["0"].forwardRules

    assert_forward_rule(forward_rules[0], "A", "0", [ "1" ])
    assert_forward_rule(forward_rules[1], "B", "0", [ "1" ])

    assert was_called == 1
    assert was_called_1 == 1



def test_forwarded_used_locally_used_on_dest_not_forwarded(monkeypatch: MonkeyPatch):
    and_a_b = Projection("AND(A,B)", 1, [ ProjectionPart("A", {}), ProjectionPart("B", {}) ],  False, False)

    plan_0 = EvaluationPlan(
        [ { "A": 1, "B": 1, "C": 1 }, { "A": 0, "B": 0, "C": 1 } ],
        {
            "0": PlanNode("0", [ ForwardRule("AND(A,B)", "0", [ "1" ]) ], {
                "AND(A,B)": [ and_a_b ],
                "AND(A,B,C)": [ Projection("AND(A,B,C)", 1, [ ProjectionPart("AND(A,B)", {}), ProjectionPart("C", {}) ], False, False) ]
            }, { "A": 1, "B": 1, "C": 1 }),
            "1": PlanNode("1", [ ], {
                "AND(A,B,C)": [ Projection("AND(A,B,C)", 1, [ProjectionPart("AND(A,B)", {}), ProjectionPart("C", {}) ], False, False) ]
            }, { "A": 0, "B": 0, "C": 1 })
        })
    
    plan_1 = deepcopy(plan_0)

    change_handler_0, change_handler_1, mock_monitor_0, _ = basic_test_setup_2(plan_0, plan_1)

    was_called = 0
    def mock_send_flink_command(type: Literal["forward_inputs", "switch_pattern", "change_rules"], parameters: List[str]):
        assert type == "forward_inputs"
        assert parameters == [ "AND(A,B)", "keep" ] # !!!
        nonlocal was_called
        was_called += 1

    was_called_1 = 0
    def mock_send_flink_command_1(type: Literal["forward_inputs", "switch_pattern", "change_rules"], parameters: List[str]):
        assert type == "switch_pattern"
        assert parameters == [ "AND(A,B)", "AND(A,B,C)", "disable" ]
        nonlocal was_called_1
        was_called_1 += 1

    monkeypatch.setattr(change_handler_0, "send_flink_control_message", mock_send_flink_command)
    monkeypatch.setattr(change_handler_1, "send_flink_control_message", mock_send_flink_command_1)

    mock_monitor_0.on_change("0", plan_0.nodes["0"].projections["AND(A,B)"][0])

    assert was_called == 1
    assert was_called_1 == 1

    assert and_a_b.is_forwarded == False
    assert and_a_b.is_used == True

    assert "AND(A,B)" in plan_0.nodes["0"].projections
    assert_projection(plan_0.nodes["0"].projections["AND(A,B)"][0], ("AND(A,B)", [("A", { "0": 1 }), ("B", { "0": 1 })]))

    assert "AND(A,B,C)" in plan_0.nodes["0"].projections
    assert_projection(plan_0.nodes["0"].projections["AND(A,B,C)"][0], ("AND(A,B,C)", [ ("AND(A,B)", { "0": 1 }), ("C", { "0": 1 }) ]))

    receiver_proj = plan_1.nodes["1"].projections["AND(A,B,C)"]

    assert len(receiver_proj) == 1

    assert_projection(receiver_proj[0], ("AND(A,B,C)", [ ("A", { "0": 1 }), ("B", { "0": 1 }), ("C", { "1": 1 }) ]))

    forward_rules = plan_0.nodes["0"].forwardRules

    assert_forward_rule(forward_rules[0], "A", "0", [ "1" ])
    assert_forward_rule(forward_rules[1], "B", "0", [ "1" ])



def test_forwarded_not_used_locally_not_used_on_target_forwarded_used_on_target2_not_forwarded(monkeypatch: MonkeyPatch):
    and_a_b = Projection("AND(A,B)", 1, [ ProjectionPart("A", {}), ProjectionPart("B", {}) ], False, False)
    and_a_b_c = Projection("AND(A,B,C)", 1, [ ProjectionPart("AND(A,B)", {}), ProjectionPart("C", {}) ], False, False)
    
    plan_0 = EvaluationPlan([ { "A": 1, "B": 1, "C": 0 }, { "A": 0, "B": 0, "C": 0 }, { "A": 0, "B": 0, "C": 1 } ], { 
        "0": PlanNode(
            nodeId="0", 
            forwardRules=[ ForwardRule("AND(A,B)", "0", [ "1" ]) ], 
            projections={ "AND(A,B)": [ and_a_b ] },
            eventRates={ "A": 1, "B": 1 }
        ),
        "1": PlanNode(
            nodeId="1",
            forwardRules=[ ForwardRule("AND(A,B)", "0", [ "2" ]) ],
            projections={},
            eventRates={ "A": 0, "B": 0, "C": 0 }
        ),
        "2": PlanNode(
            nodeId="2",
            forwardRules=[],
            projections={ "AND(A,B,C)": [ and_a_b_c ] },
            eventRates={ "A": 0, "B": 0, "C": 1 }
        )
    })

    assert and_a_b.is_forwarded == True
    assert and_a_b_c.is_used == False

    plan_1 = deepcopy(plan_0)
    plan_2 = deepcopy(plan_0)

    change_handler_0, change_handler_1, change_handler_2, mock_monitor_0, _, _ = basic_test_setup_3(plan_0, plan_1, plan_2)

    was_called = 0
    def mock_send_flink_command(type: Literal["forward_inputs", "switch_pattern", "change_rules"], parameters: List[str]):
        assert type == "forward_inputs"
        assert parameters == [ "AND(A,B)", "disable" ]
        nonlocal was_called
        was_called += 1

    was_called_1 = 0
    def mock_send_flink_command_1(type: Literal["forward_inputs", "switch_pattern", "change_rules"], parameters: List[str]):
        assert type == "change_rules"
        assert parameters == [ "AND(A,B)", "0", "A", "B" ]
        nonlocal was_called_1
        was_called_1 += 1

    was_called_2 = 0
    def mock_send_flink_command_2(type: Literal["forward_inputs", "switch_pattern", "change_rules"], parameters: List[str]):
        assert type == "switch_pattern"
        assert parameters == [ "AND(A,B)", "AND(A,B,C)", "disable" ]
        nonlocal was_called_2
        was_called_2 += 1

    monkeypatch.setattr(change_handler_0, "send_flink_control_message", mock_send_flink_command)
    monkeypatch.setattr(change_handler_1, "send_flink_control_message", mock_send_flink_command_1)
    monkeypatch.setattr(change_handler_2, "send_flink_control_message", mock_send_flink_command_2)

    mock_monitor_0.on_change("0", and_a_b)

    print(plan_0.to_json_str() + "\n" + plan_1.to_json_str() + "\n" + plan_2.to_json_str())

    assert and_a_b.is_forwarded == False
    assert and_a_b.is_used == False
    assert "AND(A,B)" not in plan_0.nodes["0"].projections

    receiver_proj = plan_2.nodes["2"].projections["AND(A,B,C)"]

    assert len(receiver_proj) == 1

    assert_projection(receiver_proj[0], ("AND(A,B,C)", [ ("A", { "1": 1 }), ("B", { "1": 1 }), ("C", { "2": 1 }) ]))

    forward_rules = plan_0.nodes["0"].forwardRules

    assert_forward_rule(forward_rules[0], "A", "0", [ "1" ])
    assert_forward_rule(forward_rules[1], "B", "0", [ "1" ])

    assert len(plan_1.nodes["1"].projections) == 0
    forward_rules = plan_1.nodes["1"].forwardRules

    assert_forward_rule(forward_rules[0], "A", "0", [ "2" ])
    assert_forward_rule(forward_rules[1], "B", "0", [ "2" ])

    assert was_called == 1
    assert was_called_1 == 1
    assert was_called_2 == 1



def test_forwarded_not_used_locally_used_on_target_forwarded_used_on_target2_not_forwarded(monkeypatch: MonkeyPatch):
    and_a_b = Projection("AND(A,B)", 1, [ ProjectionPart("A", {}), ProjectionPart("B", {}) ], False, False)
    and_a_b_c = Projection("AND(A,B,C)", 1, [ ProjectionPart("AND(A,B)", {}), ProjectionPart("C", {}) ], False, False)
    
    plan_0 = EvaluationPlan([ { "A": 1, "B": 1, "C": 0 }, { "A": 0, "B": 0, "C": 1 }, { "A": 0, "B": 0, "C": 1 } ], { 
        "0": PlanNode(
            nodeId="0", 
            forwardRules=[ ForwardRule("AND(A,B)", "0", [ "1" ]) ], 
            projections={ "AND(A,B)": [ and_a_b ] },
            eventRates={ "A": 1, "B": 1 }
        ),
        "1": PlanNode(
            nodeId="1",
            forwardRules=[ ForwardRule("AND(A,B)", "0", [ "2" ]) ],
            projections={ "AND(A,B,C)": [ deepcopy(and_a_b_c) ] },
            eventRates={ "A": 0, "B": 0, "C": 1 }
        ),
        "2": PlanNode(
            nodeId="2",
            forwardRules=[],
            projections={ "AND(A,B,C)": [ and_a_b_c ] },
            eventRates={ "A": 0, "B": 0, "C": 1 }
        )
    })

    assert and_a_b.is_forwarded == True
    assert and_a_b_c.is_used == False

    plan_1 = deepcopy(plan_0)
    plan_2 = deepcopy(plan_0)

    change_handler_0, change_handler_1, change_handler_2, mock_monitor_0, _, _ = basic_test_setup_3(plan_0, plan_1, plan_2)

    was_called = 0
    def mock_send_flink_command(type: Literal["forward_inputs", "switch_pattern", "change_rules"], parameters: List[str]):
        assert type == "forward_inputs"
        assert parameters == [ "AND(A,B)", "disable" ]
        nonlocal was_called
        was_called += 1

    was_called_1 = 0
    def mock_send_flink_command_1(type: Literal["forward_inputs", "switch_pattern", "change_rules"], parameters: List[str]):
        nonlocal was_called_1
        if was_called_1 == 0:
            assert type == "change_rules"
            assert parameters == [ "AND(A,B)", "0", "A", "B" ]
        elif was_called == 1:
            assert type == "switch_pattern"
            assert parameters == [ "AND(A,B)", "AND(A,B,C)", "disable" ]

        was_called_1 += 1

    was_called_2 = 0
    def mock_send_flink_command_2(type: Literal["forward_inputs", "switch_pattern", "change_rules"], parameters: List[str]):
        assert type == "switch_pattern"
        assert parameters == [ "AND(A,B)", "AND(A,B,C)", "disable" ]
        nonlocal was_called_2
        was_called_2 += 1

    monkeypatch.setattr(change_handler_0, "send_flink_control_message", mock_send_flink_command)
    monkeypatch.setattr(change_handler_1, "send_flink_control_message", mock_send_flink_command_1)
    monkeypatch.setattr(change_handler_2, "send_flink_control_message", mock_send_flink_command_2)

    mock_monitor_0.on_change("0", and_a_b)

    assert and_a_b.is_forwarded == False
    assert and_a_b.is_used == False
    assert "AND(A,B)" not in plan_0.nodes["0"].projections

    receiver_proj = plan_1.nodes["1"].projections["AND(A,B,C)"]

    assert len(receiver_proj) == 1

    assert_projection(receiver_proj[0], ("AND(A,B,C)", [ ("A", { "0": 1 }), ("B", { "0": 1 }), ("C", { "1": 1 }) ]))

    receiver_proj = plan_2.nodes["2"].projections["AND(A,B,C)"]

    assert len(receiver_proj) == 1

    assert_projection(receiver_proj[0], ("AND(A,B,C)", [ ("A", { "1": 1 }), ("B", { "1": 1 }), ("C", { "2": 1 }) ]))

    forward_rules = plan_0.nodes["0"].forwardRules

    assert_forward_rule(forward_rules[0], "A", "0", [ "1" ])
    assert_forward_rule(forward_rules[1], "B", "0", [ "1" ])

    forward_rules = plan_1.nodes["1"].forwardRules

    assert_forward_rule(forward_rules[0], "A", "0", [ "2" ])
    assert_forward_rule(forward_rules[1], "B", "0", [ "2" ])

    assert was_called == 1
    assert was_called_1 == 2
    assert was_called_2 == 1



def test_target_with_two_sources(monkeypatch: MonkeyPatch):
    and_a_b = Projection("AND(A,B)", 1, [ ProjectionPart("A", {}), ProjectionPart("B", {}) ], False, False)

    plan_0 = EvaluationPlan([ { "A": 1, "B": 1, "C": 0 }, { "A": 0, "B": 0, "C": 1 }, { "A": 1, "B": 1, "C": 0 } ], {
        "0": PlanNode("0", [ ForwardRule("AND(A,B)", "0", [ "1" ]) ], {
            "AND(A,B)": [ and_a_b ]
        }, { "A": 1, "B": 1, "C": 0 }),
        "1": PlanNode("1", [ ForwardRule("AND(A,B,C)", "1", [ "3" ]) ], {
            "AND(A,B,C)": [ Projection("AND(A,B,C)", 1, [
                ProjectionPart("AND(A,B)", {}), ProjectionPart("C", {})
            ], False, False)]
        }, { "A": 0, "B": 0, "C": 1 }),
        "2": PlanNode("2", [ ForwardRule("AND(A,B)", "2", [ "1" ]) ], {
            "AND(A,B)": [ Projection("AND(A,B)", 1, [
                ProjectionPart("A", {}), ProjectionPart("B", {})
            ], False, False) ]
        }, { "A": 1, "B": 1, "C": 0 }),
        "3": PlanNode("3", [], {}, { "A": 0, "B": 0, "C": 0 })
    })

    plan_1 = deepcopy(plan_0)
    plan_2 = deepcopy(plan_0)

    mock_monitor_0 = MockNodeMonitor("0")
    mock_monitor_1 = MockNodeMonitor("1")
    mock_monitor_2 = MockNodeMonitor("2")

    topology_node_0 = TopologyConfigNode("0", "localhost", 5000, 5001, {}, [])
    topology_node_1 = TopologyConfigNode("1", "localhost", 5002, 5003, {}, [])
    topology_node_2 = TopologyConfigNode("2", "localhost", 5004, 5005, {}, [])
    mock_metrics = MockMetrics()

    mock_control_messenger_1 = MockControlMessenger()
    def send_to_1(msg: ControlMessage):
        msg.forwardNode = "0"
        mock_control_messenger_1.on_received(msg)

    mock_control_messenger_2 = MockControlMessenger(send_to_1)
    mock_control_messenger_0 = MockControlMessenger(send_to_1)
    
    change_handler_0 = SpecialChangeHandler(mock_control_messenger_0, mock_monitor_0, plan_0, topology_node_0, mock_metrics, False) # type: ignore
    change_handler_1 = SpecialChangeHandler(mock_control_messenger_1, mock_monitor_1, plan_1, topology_node_1, mock_metrics, False) # type: ignore
    change_handler_2 = SpecialChangeHandler(mock_control_messenger_2, mock_monitor_2, plan_2, topology_node_2, mock_metrics, False) # type: ignore

    was_called = 0
    def mock_send_flink_command(type: Literal["forward_inputs", "switch_pattern", "change_rules"], parameters: List[str]):
        assert type == "forward_inputs"
        assert parameters == [ "AND(A,B)", "disable" ]
        nonlocal was_called
        was_called += 1

    was_called_1 = 0
    def mock_send_flink_command_1(type: Literal["forward_inputs", "switch_pattern", "change_rules"], parameters: List[str]):
        assert type == "switch_pattern"
        assert parameters == [ "AND(A,B)", "AND(A,B,C)", "keep" ]
        nonlocal was_called_1
        was_called_1 += 1

    def mock_send_flink_command_2(type: Literal["forward_inputs", "switch_pattern", "change_rules"], parameters: List[str]):
        assert False, "Node 2 should not send engine commands."

    monkeypatch.setattr(change_handler_0, "send_flink_control_message", mock_send_flink_command)
    monkeypatch.setattr(change_handler_1, "send_flink_control_message", mock_send_flink_command_1)
    monkeypatch.setattr(change_handler_2, "send_flink_control_message", mock_send_flink_command_2)

    mock_monitor_0.on_change("0", and_a_b)

    print(plan_0.to_json_str() + "\n" + plan_1.to_json_str() + "\n" + plan_2.to_json_str())

    assert and_a_b.is_forwarded == False
    assert "AND(A,B)" not in plan_0.nodes["0"].projections

    forward_rules = plan_0.nodes["0"].forwardRules

    assert_forward_rule(forward_rules[0], "A", "0", [ "1" ])
    assert_forward_rule(forward_rules[1], "B", "0", [ "1" ])

    assert len(plan_1.nodes["1"].projections["AND(A,B,C)"]) == 2

    assert_projection(plan_1.nodes["1"].projections["AND(A,B,C)"][0], ("AND(A,B,C)", [ ("AND(A,B)", { "2": 1 }), ("C", { "1": 1 }) ]))
    assert_projection(plan_1.nodes["1"].projections["AND(A,B,C)"][1], ("AND(A,B,C)", [ ("A", { "0": 1 }), ("B", { "0": 1 }), ("C", { "1": 1 }) ]))

    assert_forward_rule(plan_1.nodes["1"].forwardRules[0], "AND(A,B,C)", "1", [ "3" ])

    assert was_called == 1
    assert was_called_1 == 1


def test_target_with_harmful_projection(monkeypatch: MonkeyPatch):
    and_a_b = Projection("AND(A,B)", 1, [ ProjectionPart("A", {}), ProjectionPart("B", {}) ], False, False)

    plan_0 = EvaluationPlan([ { "A": 1, "B": 1, "C": 0 }, { "A": 1, "B": 1, "C": 1 }, { "A": 0, "B": 0, "C": 0 } ], {
        "0": PlanNode("0", [ ForwardRule("AND(A,B)", "0", [ "1" ]) ], {
            "AND(A,B)": [ and_a_b ]
        }, { "A": 1, "B": 1, "C": 0 }),
        "1": PlanNode("1", [ ForwardRule("AND(A,B,C)", "1", [ "2" ]) ], {
            "AND(A,B)": [ Projection("AND(A,B)", 1, [ ProjectionPart("A", {}), ProjectionPart("B", {}) ], False, False) ],
            "AND(A,B,C)": [ Projection("AND(A,B,C)", 1, [ ProjectionPart("AND(A,B)", {}), ProjectionPart("C", {}) ], False, False) ]
        }, { "A": 1, "B": 1, "C": 1 }),
        "2": PlanNode("2", [], {}, { "A": 0, "B": 0, "C": 0 })
    })

    plan_1 = deepcopy(plan_0)
    plan_2 = deepcopy(plan_0)

    change_handler_0, change_handler_1, change_handler_2, mock_monitor_0, _, _ = basic_test_setup_3(plan_0, plan_1, plan_2)

    was_called = 0
    def mock_send_flink_command(type: Literal["forward_inputs", "switch_pattern", "change_rules"], parameters: List[str]):
        assert type == "forward_inputs"
        assert parameters == [ "AND(A,B)", "disable" ]
        nonlocal was_called
        was_called += 1

    def mock_send_flink_command_1(type: Literal["forward_inputs", "switch_pattern", "change_rules"], parameters: List[str]):
        assert False, "Node 1 should not send engine commands."

    def mock_send_flink_command_2(type: Literal["forward_inputs", "switch_pattern", "change_rules"], parameters: List[str]):
        assert False, "Node 2 should not send engine commands."
    
    monkeypatch.setattr(change_handler_0, "send_flink_control_message", mock_send_flink_command)
    monkeypatch.setattr(change_handler_1, "send_flink_control_message", mock_send_flink_command_1)
    monkeypatch.setattr(change_handler_2, "send_flink_control_message", mock_send_flink_command_2)

    mock_monitor_0.on_change("0", and_a_b)

    assert and_a_b.is_forwarded == False
    assert and_a_b.is_used == False

    assert len(list(itertools.chain.from_iterable(plan_1.nodes["1"].projections.values()))) == 2

    assert_projection(plan_1.nodes["1"].projections["AND(A,B)"][0], ("AND(A,B)", [ ("A", { "0": 1, "1": 1 }), ("B", { "0": 1, "1": 1 }) ]))
    assert_projection(plan_1.nodes["1"].projections["AND(A,B,C)"][0], ("AND(A,B,C)", [ ("AND(A,B)", { "1": 1 }), ("C", { "1": 1 }) ]))

    assert len(plan_0.nodes["0"].forwardRules) == 2
    assert_forward_rule(plan_0.nodes["0"].forwardRules[0], "A", "0", [ "1" ])
    assert_forward_rule(plan_0.nodes["0"].forwardRules[1], "B", "0", [ "1" ])

    assert len(plan_1.nodes["1"].forwardRules) == 1
    assert_forward_rule(plan_1.nodes["1"].forwardRules[0], "AND(A,B,C)", "1", [ "2" ])

    assert was_called == 1



def test_target_with_harmful_projection_but_different_combination(monkeypatch: MonkeyPatch):
    and_a_b_c = Projection("AND(A,B,C)", 1, [ ProjectionPart("AND(A,B)", {}), ProjectionPart("C", {}) ], False, False)

    plan_0 = EvaluationPlan([ { "A": 1, "B": 1, "C": 1, "D": 0 }, { "A": 1, "B": 1, "C": 1, "D": 0 }, { "A": 0, "B": 0, "C": 0, "D": 1 } ], {
        "0": PlanNode("0", [ ForwardRule("AND(A,B)", "0", ["1"]), ForwardRule("AND(A,B,C)", "0", [ "1" ]) ], {
            "AND(A,B)": [ Projection("AND(A,B)", 1, [ ProjectionPart("A", {}), ProjectionPart("B", {}) ], False, False) ],
            "AND(A,B,C)": [ and_a_b_c ]
        }, { "A": 1, "B": 1, "C": 1, "D": 0 }),
        "1": PlanNode("1", [ ForwardRule("AND(A,B)", "0", ["2"]), ForwardRule("AND(A,B,C)", "1", [ "2" ]), ForwardRule("AND(A,B,C)", "0", [ "2" ]) ], {
            "AND(B,C)": [ Projection("AND(B,C)", 1, [ ProjectionPart("B", {}), ProjectionPart("C", {}) ], False, False) ],
            "AND(A,B,C)": [ Projection("AND(A,B,C)", 1, [ ProjectionPart("A", {}), ProjectionPart("AND(B,C)", {}) ], False, False) ]
        }, { "A": 1, "B": 1, "C": 1, "D": 0 }),
        "2": PlanNode("2", [], {
            "AND(A,B,D)": [ Projection("AND(A,B,D)", 1, [ ProjectionPart("AND(A,B)", {}), ProjectionPart("D", {}) ], False, False) ],
            "AND(A,B,C,D)": [ Projection("AND(A,B,C,D)", 1, [ ProjectionPart("AND(A,B,C)", {}), ProjectionPart("D", {}) ], False, False) ]
        }, { "A": 0, "B": 0, "C": 0, "D": 1 })
    })

    print(plan_0.to_json_str())

    plan_1 = deepcopy(plan_0)
    plan_2 = deepcopy(plan_0)

    change_handler_0, change_handler_1, change_handler_2, mock_monitor_0, _, _ = basic_test_setup_3(plan_0, plan_1, plan_2)

    was_called = 0
    def mock_send_flink_command(type: Literal["forward_inputs", "switch_pattern", "change_rules"], parameters: List[str]):
        assert type == "forward_inputs"
        assert parameters == [ "AND(A,B,C)", "disable" ]
        nonlocal was_called
        was_called += 1

    was_called_1 = 0
    def mock_send_flink_command_1(type: Literal["forward_inputs", "switch_pattern", "change_rules"], parameters: List[str]):
        assert type == "change_rules"
        assert parameters == [ "AND(A,B,C)", "0", "AND(A,B)", "C" ]
        nonlocal was_called_1
        was_called_1 += 1

    was_called_2 = 0
    def mock_send_flink_command_2(type: Literal["forward_inputs", "switch_pattern", "change_rules"], parameters: List[str]):
        assert type == "switch_pattern"
        assert parameters == [ "AND(A,B,C)", "AND(A,B,C,D)", "keep" ]
        nonlocal was_called_2
        was_called_2 += 1
    
    monkeypatch.setattr(change_handler_0, "send_flink_control_message", mock_send_flink_command)
    monkeypatch.setattr(change_handler_1, "send_flink_control_message", mock_send_flink_command_1)
    monkeypatch.setattr(change_handler_2, "send_flink_control_message", mock_send_flink_command_2)

    mock_monitor_0.on_change("0", and_a_b_c)

    print(plan_0.to_json_str() + "\n" + plan_1.to_json_str() + "\n" + plan_2.to_json_str())
    
    assert was_called == 1
    assert was_called_1 == 1
    assert was_called_2 == 1

    assert and_a_b_c.is_forwarded == False

    assert_forward_rule(plan_0.nodes["0"].forwardRules[0], "AND(A,B)", "0", [ "1" ])
    assert_forward_rule(plan_0.nodes["0"].forwardRules[1], "C", "0", [ "1" ])

    assert len(plan_1.nodes["1"].projections["AND(A,B,C)"]) == 1

    assert_projection(plan_1.nodes["1"].projections["AND(B,C)"][0], ("AND(B,C)", [ ("B", { "1": 1 }), ("C", { "1": 1 }) ]))
    assert_projection(plan_1.nodes["1"].projections["AND(A,B,C)"][0], ("AND(A,B,C)", [ ("A", { "1": 1 }), ("AND(B,C)", { "1": 1 }) ]))

    assert len(plan_1.nodes["1"].forwardRules) == 3

    assert_forward_rule(plan_1.nodes["1"].forwardRules[0], "AND(A,B)", "0", [ "2" ])
    assert_forward_rule(plan_1.nodes["1"].forwardRules[1], "AND(A,B,C)", "1", [ "2" ])
    assert_forward_rule(plan_1.nodes["1"].forwardRules[2], "C", "0", [ "2" ])

    assert len(list(itertools.chain.from_iterable(plan_2.nodes["2"].projections.values()))) == 3
    assert_projection(plan_2.nodes["2"].projections["AND(A,B,D)"][0], ("AND(A,B,D)", [ ("AND(A,B)", { "1": 1 }), ("D", { "2": 1 }) ]))
    assert_projection(plan_2.nodes["2"].projections["AND(A,B,C,D)"][0], ("AND(A,B,C,D)", [ ("AND(A,B,C)", { "1": 1 }), ("D", { "2": 1 }) ]))
    assert_projection(plan_2.nodes["2"].projections["AND(A,B,C,D)"][1], ("AND(A,B,C,D)", [ ("AND(A,B)", { "1": 1 }), ("C", { "1": 1 }), ("D", { "2": 1 }) ]))

    assert len(plan_2.nodes["2"].forwardRules) == 0

