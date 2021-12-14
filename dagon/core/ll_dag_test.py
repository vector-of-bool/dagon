from __future__ import annotations

import pytest

from . import ll_dag

StringGraph = ll_dag.LowLevelDAG[str]


def test_create_empty():
    g = StringGraph()
    assert set(g.all_nodes) == set()
    assert set(g.ready_nodes) == set()
    assert 'foo' not in g


def test_add_item():
    g = StringGraph(nodes=('foo', 'bar'), edges=[
        ('foo', 'bar'),
    ])
    assert 'foo' in g
    assert 'bar' in g
    assert set(g.ready_nodes) == {'foo'}


def test_finish_item():
    g = StringGraph(nodes=['foo', 'bar'], edges=[
        ('foo', 'bar'),
    ])
    g.mark_finished('foo')
    assert set(g.ready_nodes) == {'bar'}


def test_finish_item_two_chain():
    g = StringGraph(nodes={'foo', 'bar', 'baz'}, edges=[
        ('foo', 'bar'),
        ('bar', 'baz'),
    ])
    g.mark_finished('foo')
    assert set(g.ready_nodes) == {'bar'}
    g.mark_finished('bar')
    assert set(g.ready_nodes) == {'baz'}
    g.mark_finished('baz')
    assert set(g.ready_nodes) == set()


def test_finish_item_multi_inputs():
    g = StringGraph(nodes=('foo', 'bar', 'baz'), edges=[
        ('foo', 'baz'),
        ('bar', 'baz'),
    ])
    assert set(g.ready_nodes) == {'foo', 'bar'}
    g.mark_finished('foo')
    assert set(g.ready_nodes) == {'bar'}
    g.mark_finished('bar')
    assert set(g.ready_nodes) == {'baz'}


def test_finish_item_multi_outputs():
    g = StringGraph(nodes=('foo', 'bar', 'baz'), edges=[
        ('foo', 'bar'),
        ('foo', 'baz'),
    ])
    g.mark_finished('foo')
    assert set(g.ready_nodes) == {'bar', 'baz'}


def test_finish_item_cross():
    g = StringGraph(nodes=('foo', 'bar', 'baz', 'quux'),
                    edges=[
                        ('foo', 'bar'),
                        ('foo', 'baz'),
                        ('quux', 'bar'),
                        ('quux', 'baz'),
                    ])
    assert set(g.ready_nodes) == {'foo', 'quux'}
    g.mark_finished('foo')
    assert set(g.ready_nodes) == {'quux'}
    g.mark_finished('quux')
    assert set(g.ready_nodes) == {'bar', 'baz'}


def test_detect_cycles():
    g = StringGraph(nodes=('foo', 'bar', 'baz', 'quux'), edges=[
        ('foo', 'bar'),
        ('foo', 'baz'),
        ('baz', 'quux'),
    ])
    with pytest.raises(ll_dag.CycleError, match="['quux', 'baz', 'quux']"):
        g.add(edges=[('quux', 'baz')])
    with pytest.raises(ll_dag.CycleError, match="['quux', 'baz', 'foo', 'quux']"):
        g.add(edges=[('quux', 'foo')])
    g.add(edges=[('quux', 'bar')])


def test_detect_cycles_2():
    g = StringGraph(nodes=['foo', 'bar', 'baz'])
    g.add(edges=[
        ('foo', 'bar'),
        ('bar', 'baz'),
    ])
    with pytest.raises(ll_dag.CycleError):
        g.add(edges=[('baz', 'foo')])
    with pytest.raises(ll_dag.CycleError):
        g.add(edges=[('bar', 'foo')])


def test_detect_not_cycle():
    g = StringGraph(nodes=['foo', 'bar', 'baz', 'quux'], edges=[
        ('foo', 'bar'),
        ('baz', 'quux'),
    ])

    g.add(edges=[('bar', 'baz')])


def test_self_dep():
    g = StringGraph()
    with pytest.raises(ll_dag.CycleError, match="['foo', 'foo']"):
        g.add(edges=[('foo', 'foo')])


def test_add_edge_on_finished_error():
    g = StringGraph(nodes=['foo', 'bar'], edges=[('foo', 'bar')])
    assert set(g.ready_nodes) == {'foo'}
    g.mark_finished('foo')
    g.add(nodes=['baz'])
    assert set(g.ready_nodes) == {'bar', 'baz'}
    with pytest.raises(ll_dag.GraphStateError, match='foo'):
        g.add(edges=[('baz', 'foo')])


def test_add_dup_edge_error():
    g = StringGraph(nodes=['foo', 'bar'])
    g.add(edges=[('foo', 'bar')])
    assert set(g.ready_nodes) == {'foo'}
    with pytest.raises(ll_dag.DuplicateEdgeError, match="from_='foo', to='bar'"):
        g.add(edges=[('foo', 'bar')])


def test_add_dup_node_error():
    g = StringGraph(nodes=['foo'])
    with pytest.raises(ll_dag.DuplicateNodeError, match='foo'):
        g.add(nodes=['foo'])


def test_missing_node_error():
    g = StringGraph(nodes=['foo', 'bar'])
    with pytest.raises(ll_dag.MissingNodeError, match='quux'):
        g.add(edges=[('quux', 'bar')])


def test_random_large_graph():
    import random
    items: list[str] = ['foo', 'bar', 'baz']
    g = StringGraph(nodes=items)
    count = 1000
    for v in range(count):
        g.add(nodes=[str(v)])
        items.append(str(v))

    for _ in range(count):
        first = random.randrange(0, len(items) - 4)
        second = random.randrange(first + 1, len(items) - 1)
        try:
            g.add(edges=[(items[first], items[second])])
        except ll_dag.DuplicateEdgeError:
            pass


def test_restore():
    g = StringGraph()
    assert set(g.ready_nodes) == set()
    with pytest.raises(ll_dag.DuplicateNodeError):
        g.add(nodes=['foo', 'bar', 'bar', 'baz'])
    assert set(g.ready_nodes) == set()
    assert list(g.all_nodes) == []

    g.add(nodes=['foo', 'bar', 'baz'], edges=[('foo', 'bar'), ('bar', 'baz')])
    assert set(g.ready_nodes) == {'foo'}
    with pytest.raises(ll_dag.CycleError):
        g.add(edges=[('baz', 'foo')])
    assert set(g.ready_nodes) == {'foo'}


def test_copy():
    g = StringGraph(nodes=['foo', 'baz', 'bar'], edges=[('foo', 'bar'), ('bar', 'baz')])
    assert set(g.ready_nodes) == {'foo'}
    g2 = g.copy()
    g2.mark_running('foo')
    assert set(g.ready_nodes) == {'foo'}
    assert set(g2.ready_nodes) == set()
    g2.mark_finished('foo')
    assert set(g.ready_nodes) == {'foo'}
    assert set(g2.ready_nodes) == {'bar'}
    g2.mark_finished('bar')
    assert set(g.ready_nodes) == {'foo'}
    assert set(g2.ready_nodes) == {'baz'}
    g.add(nodes=['quux'], edges=[('baz', 'quux')])
    assert set(g.all_nodes) == {'foo', 'bar', 'baz', 'quux'}
    assert set(g2.all_nodes) == {'foo', 'bar', 'baz'}


def test_sameness():
    import dagon.core.ll_dag
    assert dagon.core.ll_dag.LowLevelDAG is ll_dag.LowLevelDAG  # type: ignore
