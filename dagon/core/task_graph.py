"""
Provides a low-level generic task graph implementation.
"""
from __future__ import annotations

import copy
import enum
from dataclasses import dataclass, field
from typing import Any, Dict, Generic, Hashable, Iterable, Sequence, TypeVar

from typing_extensions import TypeAlias

TaskT = TypeVar('TaskT', bound=Hashable)
"""
A type parameter for directed graph nodes. Each node must be hashable.
"""


class GraphError(RuntimeError):
    """
    Base class for any errors involving the directed graph
    """


@dataclass
class GraphStateError(RuntimeError):
    node: Any


@dataclass
class CycleError(GraphError, Generic[TaskT]):
    hint: Sequence[TaskT]


@dataclass()
class DuplicateNodeError(GraphError):
    node: Any


@dataclass()
class DuplicateEdgeError(GraphError):
    edge: Edge[Any]


@dataclass()
class MissingNodeError(GraphError):
    node: Any


class TaskState(enum.Enum):
    Pending = 0
    Running = 1
    Finished = 2


@dataclass()
class _GraphNode(Generic[TaskT]):
    """
    A node in the graph, containing an associated value.
    """
    #: The object associated with this node
    value: TaskT
    #: The number of pending inputs to this node in the current execution
    pending_inputs: int = 0
    #: The order/"depth" of the node in the graph
    order: int = 0
    #: The current state of the graph
    state: TaskState = TaskState.Pending
    #: The objects that depends on this node in the graph
    outs: set[TaskT] = field(default_factory=set)
    #: The objects upon which this node depends
    ins: set[TaskT] = field(default_factory=set)

    def __deepcopy__(self, memo: Any) -> _GraphNode[TaskT]:
        return _GraphNode(self.value, self.pending_inputs, self.order, self.state, set(self.outs), set(self.ins))

    @property
    def is_ready(self) -> bool:
        """Determine whether this node is ready to run"""
        return self.state is TaskState.Pending and self.pending_inputs == 0


@dataclass(frozen=True)
class Edge(Generic[TaskT]):
    """
    A edge in the graph representing a dependency of one node value on another
    """
    from_: TaskT
    to: TaskT


_NodeMap: TypeAlias = Dict[TaskT, _GraphNode[TaskT]]
"""A map of node values to the metadata for that node"""


class TaskGraph(Generic[TaskT]):
    """
    A generic directed acyclic task-graph. It is a "task" graph because a node
    can be marked "finished", and one can query the graph for the nodes that do
    not have any un-finished inputs (Meaning the task is "ready").

    :param nodes: The initial nodes of the graph.
    :param edges: The initial edges of the graph.

    .. seealso:: :func:`.add`
    """
    def __init__(self, *, nodes: Iterable[TaskT] = (), edges: Iterable[Edge[TaskT] | tuple[TaskT, TaskT]] = ()) -> None:
        #: All nodes in the graph
        self._nodes: _NodeMap[TaskT] = {}
        #: The set of nodes that are ready to be processed
        self._ready_nodes: set[TaskT] = set()
        #: all of the edges in the graph
        self._edges: set[Edge[TaskT]] = set()
        # Begin by loading up all the initial nodes and edges
        self.add(nodes=nodes, edges=edges)

    def add(self, *, nodes: Iterable[TaskT] = (), edges: Iterable[Edge[TaskT] | tuple[TaskT, TaskT]] = ()) -> None:
        """
        Add content to the graph.

        :param nodes: Nodes to add to the graph.
        :param edges: Edges to add to the graph.
        """
        # Duplicate the nodes so that we can modify them in-place. Exceptions will leave the
        # graph unchanged.
        ready_nodes = set(self._ready_nodes)
        repl: _NodeMap[TaskT] = {}
        more_edges: set[Edge[TaskT]] = set()

        # First add all the nodes
        for n in nodes:
            if n in self._nodes or n in repl:
                raise DuplicateNodeError(n)
            repl[n] = _GraphNode(n)
            ready_nodes.add(n)

        # Now add all the edges
        for e in edges:
            if not isinstance(e, Edge):
                e = Edge(e[0], e[1])
            self._add_edge(e, ready_nodes, repl)
            more_edges.add(e)

        # Store the nodes and update the ready-nodes
        self._nodes.update(repl)
        self._edges |= more_edges
        self._ready_nodes = ready_nodes

    def copy(self) -> TaskGraph[TaskT]:
        return copy.deepcopy(self)

    def mark_running(self, node: TaskT) -> None:
        """
        Mark a node as "running"
        """
        gnode = self._nodes.get(node)
        assert gnode is not None, ('mark_running() a node which is not in the graph', node)
        assert gnode.pending_inputs == 0, ('mark_running() a node that still has pending inputs', gnode)
        assert gnode.value in self._ready_nodes
        self._ready_nodes.remove(gnode.value)
        gnode.state = TaskState.Running

    def mark_finished(self, node: TaskT) -> None:
        """
        Mark a node as "finished." This will update the ready-nodes of this graph.
        """
        gnode = self._nodes.get(node)
        assert gnode is not None, ('mark_finished() a node which is not in the graph', node)
        assert gnode.pending_inputs == 0, ('mark_finished() a node that still has pending inputs', gnode)
        assert gnode.state is not TaskState.Finished, ('mark_finished() a node that was already marked as finished',
                                                       gnode)
        if gnode.state is TaskState.Pending:
            #: A node can be marked as finished if it was pending but not running, just skip that state
            self._ready_nodes.remove(gnode.value)
        gnode.state = TaskState.Finished
        for out in gnode.outs:
            out_node = self._nodes.get(out)
            assert out_node is not None, 'Malformed task graph'
            assert node in out_node.ins
            assert out_node.pending_inputs > 0
            out_node.pending_inputs -= 1
            if out_node.pending_inputs == 0:
                # Finishing this node has caused a dependency to become ready
                self._ready_nodes.add(out_node.value)

    @property
    def ready_nodes(self) -> Iterable[TaskT]:
        """The node values in the graph that have no unfinished inputs"""
        return iter(self._ready_nodes)

    @property
    def all_nodes(self) -> Iterable[TaskT]:
        """All nodes currently in the task graph"""
        return self._nodes.keys()

    @property
    def all_edges(self) -> Iterable[Edge[TaskT]]:
        """Iterate all the edges in the graph"""
        return iter(self._edges)

    @property
    def has_ready_nodes(self) -> bool:
        return bool(self._ready_nodes)

    def dependencies_of(self, node: TaskT) -> Iterable[TaskT]:
        return iter(self._nodes[node].ins)

    def dependents_of(self, node: TaskT) -> Iterable[TaskT]:
        return iter(self._nodes[node].outs)

    def state_of(self, node: TaskT) -> TaskState:
        return self._nodes[node].state

    def _add_edge(self, edge: Edge[TaskT], ready_nodes: set[TaskT], repl: _NodeMap[TaskT]) -> None:
        """
        Introduce a new edge to the graph. This requires that all output nodes be pending.

        'repl' is the map of nodes that is being updated and will replace our own map of nodes once
        the update is safely complete.
        """
        from_node = repl.get(edge.from_) or copy.deepcopy(self._nodes.get(edge.from_))
        to_node = repl.get(edge.to) or copy.deepcopy(self._nodes.get(edge.to))

        if edge.from_ == edge.to:
            # Edge points to itself
            raise CycleError([edge.to, edge.to])
        if from_node is None:
            # from-node doesn't exist
            raise MissingNodeError(edge.from_)
        if to_node is None:
            # to-node doesn't exist
            raise MissingNodeError(edge.to)

        if to_node.state is not TaskState.Pending:
            # to-node has already been marked finished/running, so the dependency is broken
            raise GraphStateError(to_node.value)

        if edge.to in from_node.outs:
            # The to-node is already connected to the from-node
            raise DuplicateEdgeError(edge)

        repl[from_node.value] = from_node
        repl[to_node.value] = to_node

        assert edge.from_ not in to_node.ins

        # Connect input and output nodes
        from_node.outs.add(edge.to)
        to_node.ins.add(edge.from_)

        # Recalculate the order of the from-node
        if to_node.order >= from_node.order:
            from_node.order = to_node.order + 1
            # Update the order number. This will also detect and raise on graph cycles
            self._propagate_order(repl, from_node, set(), [from_node.value])

        # If the from-node is not already marked as "finished", then the to-node
        # has an additional pending input
        if from_node.state is not TaskState.Finished:
            to_node.pending_inputs += 1
            if to_node.pending_inputs == 1:
                # We made the node un-ready
                ready_nodes.remove(to_node.value)

    def _get_node(self, repl: _NodeMap[TaskT], k: TaskT) -> _GraphNode[TaskT]:
        n = repl.get(k)
        if n:
            return n
        n = self._nodes.get(k)
        assert n
        return n

    def _propagate_order(self, repl: _NodeMap[TaskT], node: _GraphNode[TaskT], visited: set[TaskT],
                         stack: list[TaskT]) -> None:
        pending = (n for n in node.ins if n not in visited)
        for inp in pending:
            visited.add(inp)
            from_node = self._get_node(repl, inp)
            self._recalc_order(repl, from_node, visited, stack)

    def _recalc_order(self, repl: _NodeMap[TaskT], node: _GraphNode[TaskT], visited: set[TaskT],
                      stack: list[TaskT]) -> None:
        if node.value == stack[0]:
            raise CycleError(stack)
        out_max = max(self._get_node(repl, n).order for n in node.outs)
        new_order = max(out_max + 1, node.order)
        assert new_order >= node.order
        if new_order > node.order:
            n = repl[node.value] = copy.deepcopy(node)
            n.order = new_order
            stack.append(node.value)
            self._propagate_order(repl, node, visited, stack)
            stack.pop()

    def __contains__(self, value: TaskT) -> bool:
        "Check whether the named value is already a node in the graph"
        return value in self._nodes


class TaskGraphView(Generic[TaskT]):
    """
    Obtain a readonly view of a task graph.

    :param graph: The graph to view
    """
    def __init__(self, graph: TaskGraph[TaskT]) -> None:
        self._graph = graph

    def dependencies_of(self, node: TaskT) -> Iterable[TaskT]:
        "Get the dependencies of the given node"
        return self._graph.dependencies_of(node)

    def dependents_of(self, node: TaskT) -> Iterable[TaskT]:
        "Get the dependents of the given node"
        return self._graph.dependents_of(node)

    def nodes(self) -> Iterable[TaskT]:
        "Iterate all the nodes in the graph"
        return self._graph.all_nodes

    def edges(self) -> Iterable[Edge[TaskT]]:
        "Iterate all the edges in the graph"
        return self._graph.all_edges
