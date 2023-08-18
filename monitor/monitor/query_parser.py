from abc import abstractmethod, ABC
from collections import deque
import re
from typing import Dict, List, Tuple, Union 

class SubQuery(ABC):
    raw: str # debug display

    @abstractmethod
    def calc_rate(self, eventRates: Dict[str, int]) -> int:
        pass

    @abstractmethod
    def leafs(self) -> List[str]:
        pass

    @abstractmethod
    def leafs_with_parent(self) -> List[Tuple[str, "SubQuery"]]:
        pass

    @abstractmethod
    def is_sub_query(self, sub: "SubQuery"):
        pass

    def __eq__(self, __o: object) -> bool:
        if isinstance(__o, SubQuery):
            return __o.raw == self.raw
        else:
            return super().__eq__(__o)

LeafWithParent = Tuple[str, Union[SubQuery,None]]

class CompositeQuery(SubQuery):
    def __init__(self, children: List[SubQuery], raw: str) -> None:
        self.children = children
        self.raw = raw

    def leafs(self) -> List[str]:
        result = []
        for c in self.children:
            result += c.leafs()

        return result

    def leafs_with_parent(self) -> List[Tuple[str, "SubQuery"]]:
        result = []
        for c in self.children:
            if isinstance(c, PrimitiveEvent):
                result.append((c.eventType, self))
            else:
                result += c.leafs_with_parent()

        return result

class SequenceQuery(CompositeQuery):
    def calc_rate(self, eventRates: Dict[str, int]):
        prod = 1
        for c in self.children:
            prod *= c.calc_rate(eventRates)
        
        return prod

    def is_sub_query(self, sub: SubQuery):
        return False

class AndQuery(CompositeQuery):
    def calc_rate(self, eventRates: Dict[str, int]):
        prod = 1
        for c in self.children:
            prod *= c.calc_rate(eventRates)
        
        return prod * len(self.children)

    def is_sub_query(self, sub: SubQuery):
        if isinstance(sub, PrimitiveEvent):
            return any([c for c in self.children if isinstance(c, PrimitiveEvent) and c.eventType == sub.eventType])
        elif isinstance(sub, AndQuery):
            return set([c.raw for c in sub.children]).issubset(set([c.raw for c in self.children]))
        
        # TODO: Sequence
        return False

class PrimitiveEvent(SubQuery):
    eventType: str
    def __init__(self, eventType: str) -> None:
        self.eventType = eventType
        self.raw = eventType

    def calc_rate(self, eventRates: Dict[str, int]):
        return eventRates[self.eventType]

    def leafs(self) -> List[str]:
        return [self.eventType]

    def leafs_with_parent(self) -> List[LeafWithParent]:
        return [(self.eventType, None)]

    def is_sub_query(self, sub: "SubQuery"):
        return isinstance(sub, PrimitiveEvent) and sub.eventType == self.eventType

def extract_children(query: str):
    children = query[4:-1]
    childQueries: list[str] = []
    prevEnd = -1
    opened = 0

    for (i, char) in enumerate(children):
        if char == "(":
            opened += 1
        elif char == ")":
            opened -= 1
        elif char == ",":
            if opened == 0:
                childQueries.append(children[prevEnd + 1:i])
                prevEnd = i
        
        if i == len(children) - 1:
            childQueries.append(children[prevEnd + 1:i + 1])

    return childQueries

def parse_query(raw: str) -> SubQuery:
    return __parse_query(raw.replace(" ", "")) 

def __parse_query(raw: str) -> SubQuery:
    if raw.startswith("AND") or raw.startswith("SEQ"):
        # Complex event
        childQueries = extract_children(raw)
        childNodes = [parse_query(child) for child in childQueries]

        if raw.startswith("AND"):
            return AndQuery(childNodes, raw)
        else:
            return SequenceQuery(childNodes, raw)
    else:
        # Primitive event
        return PrimitiveEvent(raw)


def extract_leafs(query: str):
    return list(re.sub("AND|SEQ|[(), ]", "", query))

def get_sequence_leafs(query: SubQuery)-> List[LeafWithParent]:
    if isinstance(query, PrimitiveEvent):
        return [ (query.eventType, None) ]
    
    leafs: List[LeafWithParent] = []
    queue = deque([ query ])

    while queue:
        current = queue.popleft()
        if isinstance(current, SequenceQuery):
            leafs += current.leafs_with_parent()
        elif isinstance(current, AndQuery):
            for child in current.children:
                queue.append(child)

    return leafs


def is_sub_projection(sub: str, other: str):
    if len(sub) > len(other):
        return False
    
    subQ = parse_query(sub)
    otherQ = parse_query(other)
    return otherQ.is_sub_query(subQ)

def get_sequence_constraints(query: str, inputs: List[str]):
    if "SEQ(" not in query:
        return []

    parsed = parse_query(query)
    seqLeafs = get_sequence_leafs(parsed)

    inputSeqLeafs: dict[str, List[str]] = {}
    allLeafs: List[str] = []

    hasSequenceInput = False
    for input in inputs:
        if "SEQ(" in input:
            hasSequenceInput = True

        leafs = extract_leafs(input)
        inputSeqLeafs[input] = leafs
        allLeafs += leafs

    # Not using set, because it does not preserve order    
    intersect: List[LeafWithParent] = []
    for l in seqLeafs:
        if l[0] in allLeafs:
            intersect.append(l)

    constraints: List[List[str]] = []
    for i, prev in enumerate(intersect):
        for event in intersect[i + 1:]:
            if isinstance(prev[1], AndQuery):
                # Both leafs are children of the same AND -> no sequence
                if prev[1] == event[1]:
                    continue

                # Special case for SEQs that are children of ANDs
                # Note that event is deeper in the tree as prev or on the same level
                if isinstance(event[1], SequenceQuery):
                    continue

            # If both primitive events come from the same input, 
            # a sequence constraint is only required 
            # if a sequence is added by another input or the query itself
            if not hasSequenceInput or not any(inputKey for inputKey, inputLeafs in inputSeqLeafs.items() if prev[0] in inputLeafs and event[0] in inputLeafs):
                constraints.append([prev[0], event[0]])

    return constraints

def get_id_constraints(inputs: List[str]):
    commonEvents = set(extract_leafs(inputs[0]))
    for input in inputs[1:]:
        leafs = extract_leafs(input)
        commonEvents.intersection_update(leafs)

    return list(commonEvents)
