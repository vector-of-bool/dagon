from __future__ import annotations

from dagon import event as mod


def test_simple_event():
    ev = mod.Event[int]()
    ev.emit(12)

    r: dict[str, int | None] = {'got': None}

    conn = ev.connect(lambda n: r.update({'got': n}))
    ev.emit(42)
    assert r['got'] == 42
    ev.emit(1729)
    assert r['got'] == 1729

    ev.disconnect(conn)
    ev.emit(-21)
    assert r['got'] == 1729


def test_child_map():
    emp = mod.EventMap()
    ev = emp.register('ev', mod.Event[int]())
    emp['ev'].emit(31)

    r: dict[str, int | None] = {'got': None}

    conn = ev.connect(lambda n: r.update({'got': n}))
    emp['ev'].emit(21)
    assert r['got'] == 21

    child = emp.child()
    child['ev'].emit(84)
    assert r['got'] == 84
    ev.disconnect(conn)
