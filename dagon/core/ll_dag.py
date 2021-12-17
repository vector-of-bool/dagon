"""
Module ``dagon.core.ll_dag``
############################

This module provides a low-level generic task-aware DAG implementation.

The main class is `.LowLevelDAG`, which tracks node states and
dependencies between nodes.

No semantics are imposed on the node objects given to a `.LowLevelDAG`,
other than they must be valid as keys in a `set` or `dict` (They must be
`Hashable`).

This class neither concerns itself with "executing" a node. It is up to a user
of `.LowLevelDAG` to execute the nodes.

A simple low-level execution API can be found in `dagon.core.exec`.
"""
from __future__ import annotations

import copy
import enum
from dataclasses import dataclass, field
from typing import Any, Dict, Generic, Hashable, Iterable, Sequence, TypeVar

from typing_extensions import TypeAlias

from dagon.util import fixup_dataclass_docs

NodeT = TypeVar('NodeT', bound=Hashable)
"""
A type parameter for directed graph nodes. Each node must be hashable.
"""


class GraphError(RuntimeError):
    """
    Base class for any errors involving the directed graph
    """


@fixup_dataclass_docs
@dataclass()
class GraphStateError(RuntimeError):
    """
    Raised by an attempted operation on the graph when the graph is in an
    invalid state for that operation.
    """
    node: Any
    "The node that is in an invalid state"


@fixup_dataclass_docs
@dataclass
class CycleError(GraphError, Generic[NodeT]):
    """
    Exception raised when adding an edge to a graph would create a dependency cycle.
    """
    hint: Sequence[NodeT]
    """The sequence of nodes that were discovered forming a cycle"""


@fixup_dataclass_docs
@dataclass()
class DuplicateNodeError(GraphError):
    """
    Exception raised when attempting to add a node that already exists within the graph.
    """
    node: Any
    """The node that is already present in the graph"""


@fixup_dataclass_docs
@dataclass()
class DuplicateEdgeError(GraphError):
    """
    Exception raised when attempting to add a duplicate edge to a graph.
    """
    edge: Edge[Any]
    """The edge that was attempted to be added"""


@fixup_dataclass_docs
@dataclass()
class MissingNodeError(GraphError):
    """
    Exception raised when requesting data for a node that is not a member of the graph.
    """
    node: Any
    """The node that was requested"""


class NodeState(enum.Enum):
    """The task-state of a node within the graph"""
    Pending = 0
    "The node has not been marked running nor marked finished"
    Running = 1
    "The node has been marked as running but has not been marked finished"
    Finished = 2
    "The node has been marked as finished"


@fixup_dataclass_docs
@dataclass()
class _GraphNodeMeta(Generic[NodeT]):
    """
    A node in the graph, containing an associated value.
    """
    value: NodeT
    'The object associated with this node'
    pending_inputs: int = 0
    'The number of pending inputs to this node in the current execution'
    order: int = 0
    'The order/"depth" of the node in the graph'
    state: NodeState = NodeState.Pending
    'The current state of the graph'
    outs: set[NodeT] = field(default_factory=set)
    'The objects that depends on this node in the graph'
    ins: set[NodeT] = field(default_factory=set)
    'The objects upon which this node depends'

    def __deepcopy__(self, memo: Any) -> _GraphNodeMeta[NodeT]:
        return _GraphNodeMeta(self.value, self.pending_inputs, self.order, self.state, set(self.outs), set(self.ins))

    @property
    def is_ready(self) -> bool:
        """Determine whether this node is ready to run"""
        return self.state is NodeState.Pending and self.pending_inputs == 0


@dataclass(frozen=True)
class Edge(Generic[NodeT]):
    """
    An edge in the graph representing a dependency of one node value on another.
    """
    from_: NodeT
    "The outgoing node in the edge. This is the dependency."
    to: NodeT
    "The inbound node of the edge. This is the dependent."


_NodeMap: TypeAlias = Dict[NodeT, _GraphNodeMeta[NodeT]]
"""A map of node values to the metadata for that node"""


class LowLevelDAG(Generic[NodeT]):
    """
    A generic directed acyclic task-aware-graph. It is a *task-aware* because a
    node can be marked "finished", and one can query the graph for the nodes
    that do not have any un-finished inputs (Meaning the node is "ready").

    :param nodes: The initial nodes of the graph.
    :param edges: The initial edges of the graph.

    .. seealso:: See `.add` for parameter information
    """
    def __init__(self, *, nodes: Iterable[NodeT] = (), edges: Iterable[Edge[NodeT] | tuple[NodeT, NodeT]] = ()) -> None:
        self._nodes: _NodeMap[NodeT] = {}
        'All nodes in the graph'
        self._ready_nodes: set[NodeT] = set()
        'The set of nodes that are ready to be processed'
        self._edges: set[Edge[NodeT]] = set()
        'all of the edges in the graph'
        # Begin by loading up all the initial nodes and edges
        self.add(nodes=nodes, edges=edges)

    def add(self, *, nodes: Iterable[NodeT] = (), edges: Iterable[Edge[NodeT] | tuple[NodeT, NodeT]] = ()) -> None:
        """
        Add content to the graph.

        :param nodes: Nodes to add to the graph.
        :param edges: Edges to add to the graph.

        :raise GraphStateError: If any of the `~Edge.to` nodes in the new
            edges are not `~.NodeState.Pending`. (You cannot add a
            dependency to a running or finished node)
        :raise CycleError: If any of the added edges would create a dependency
            cycle.
        :raise DuplicateEdgeError: If any of the given edges already exist in
            this graph.
        :raise DuplicateNodeError: If any of the given nodes already exist in
            this graph.
        :raise MissingNodeError: If any of the edges refer to nodes that are
            not either already in the graph or added by `.nodes`.

        Edges may be instances of `.Edge` or tuples of node pairs, where
        the first element is the `~.Edge.from_` node and the second element is the
        `~.Edge.to` node.
        """
        # Duplicate the nodes so that we can modify them in-place. Exceptions will leave the
        # graph unchanged.
        ready_nodes = set(self._ready_nodes)
        repl: _NodeMap[NodeT] = {}
        more_edges: set[Edge[NodeT]] = set()

        # First add all the nodes
        for n in nodes:
            if n in self._nodes or n in repl:
                raise DuplicateNodeError(n)
            repl[n] = _GraphNodeMeta(n)
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

    def copy(self) -> LowLevelDAG[NodeT]:
        """
        Create a detached clone of this graph. This will copy the node metadata
        and edge metadata, but the nodes themselves will not be copied.
        """
        return copy.deepcopy(self)

    def mark_running(self, node: NodeT) -> None:
        """
        Mark a node as "running"

        .. warning:: Asserts that `node` is a member of the graph and that
            the node has zero pending inputs.
        """
        gnode = self._nodes.get(node)
        assert gnode is not None, ('mark_running() a node which is not in the graph', node)
        assert gnode.pending_inputs == 0, ('mark_running() a node that still has pending inputs', gnode)
        assert gnode.value in self._ready_nodes
        self._ready_nodes.remove(gnode.value)
        gnode.state = NodeState.Running

    def mark_finished(self, node: NodeT) -> None:
        """
        Mark a node as "finished." This will update the ready-nodes of this graph.

        .. note:: It is safe to mark a node as "finished" without having first
            marked it as running with `.mark_running`.

        .. warning:: Asserts that `node` is a member of the graph, that the
            node has no pending inputs, and that the node has not already been
            marked as "finished."
        """
        gnode = self._nodes.get(node)
        assert gnode is not None, ('mark_finished() a node which is not in the graph', node)
        assert gnode.pending_inputs == 0, ('mark_finished() a node that still has pending inputs', gnode)
        assert gnode.state is not NodeState.Finished, ('mark_finished() a node that was already marked as finished',
                                                       gnode)
        if gnode.state is NodeState.Pending:
            #: A node can be marked as finished if it was pending but not running, just skip that state
            self._ready_nodes.remove(gnode.value)
        gnode.state = NodeState.Finished
        for out in gnode.outs:
            out_node = self._nodes.get(out)
            assert out_node is not None, 'Malformed DAG'
            assert node in out_node.ins
            assert out_node.pending_inputs > 0
            out_node.pending_inputs -= 1
            if out_node.pending_inputs == 0:
                # Finishing this node has caused a dependency to become ready
                self._ready_nodes.add(out_node.value)

    @property
    def ready_nodes(self) -> Iterable[NodeT]:
        """The node values in the graph that have no unfinished inputs"""
        return iter(self._ready_nodes)

    @property
    def all_nodes(self) -> Iterable[NodeT]:
        """All nodes currently in the graph"""
        return self._nodes.keys()

    @property
    def all_edges(self) -> Iterable[Edge[NodeT]]:
        """Iterate all the edges in the graph"""
        return iter(self._edges)

    @property
    def has_ready_nodes(self) -> bool:
        """Boolean property on whether the graph has any ready nodes."""
        return bool(self._ready_nodes)

    def dependencies_of(self, node: NodeT) -> Iterable[NodeT]:
        """Iterate the direct dependencies of the given node"""
        return iter(self._nodes[node].ins)

    def dependents_of(self, node: NodeT) -> Iterable[NodeT]:
        """Iterate of the the direct dependents of the given node."""
        return iter(self._nodes[node].outs)

    def state_of(self, node: NodeT) -> NodeState:
        """Get the `.NodeState` of the given `node`"""
        return self._nodes[node].state

    def _add_edge(self, edge: Edge[NodeT], ready_nodes: set[NodeT], repl: _NodeMap[NodeT]) -> None:
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

        if to_node.state is not NodeState.Pending:
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
        if from_node.state is not NodeState.Finished:
            to_node.pending_inputs += 1
            if to_node.pending_inputs == 1:
                # We made the node un-ready
                ready_nodes.remove(to_node.value)

    def _get_node(self, repl: _NodeMap[NodeT], k: NodeT) -> _GraphNodeMeta[NodeT]:
        n = repl.get(k)
        if n:
            return n
        n = self._nodes.get(k)
        assert n
        return n

    def _propagate_order(self, repl: _NodeMap[NodeT], node: _GraphNodeMeta[NodeT], visited: set[NodeT],
                         stack: list[NodeT]) -> None:
        pending = (n for n in node.ins if n not in visited)
        for inp in pending:
            visited.add(inp)
            from_node = self._get_node(repl, inp)
            self._recalc_order(repl, from_node, visited, stack)

    def _recalc_order(self, repl: _NodeMap[NodeT], node: _GraphNodeMeta[NodeT], visited: set[NodeT],
                      stack: list[NodeT]) -> None:
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

    def __contains__(self, value: NodeT) -> bool:
        "Check whether the named value is already a node in the graph"
        return value in self._nodes


class DAGView(Generic[NodeT]):
    """
    Obtain a readonly view of a `.LowLevelDAG`.

    :param graph: The graph to view.
    """
    def __init__(self, graph: LowLevelDAG[NodeT]) -> None:
        self._graph = graph

    def dependencies_of(self, node: NodeT) -> Iterable[NodeT]:
        "Get the dependencies of the given node"
        return self._graph.dependencies_of(node)

    def dependents_of(self, node: NodeT) -> Iterable[NodeT]:
        "Get the dependents of the given node"
        return self._graph.dependents_of(node)

    @property
    def all_nodes(self) -> Iterable[NodeT]:
        "Iterate all the nodes in the graph"
        return self._graph.all_nodes

    @property
    def edges(self) -> Iterable[Edge[NodeT]]:
        "Iterate all the edges in the graph"
        return self._graph.all_edges
