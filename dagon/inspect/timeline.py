"""
Inspect a Dagon execution as an interactive timeline
"""

from __future__ import annotations

import base64
import json
from pathlib import Path
from pkg_resources import resource_filename
import itertools
from typing import Any, Mapping, Sequence, cast

from dagon.db import Database, RunID, TaskRunID
from dagon.util import first


def read_resource(filepath: str) -> bytes:
    return Path(resource_filename('dagon', filepath)).read_bytes()


def _gen_targets(db: Database, rid: RunID) -> Sequence[Mapping[str, Any]]:
    target_rows = db(
        r'''
        SELECT *
        FROM dagon_task_runs
        JOIN dagon_tasks USING(task_id)
        WHERE run_id=:rid
        ''',
        rid=rid,
    )
    return [dict(zip(row.keys(), row)) for row in target_rows]


def _gen_intervals(db: Database, rid: RunID) -> Mapping[TaskRunID, Sequence[Mapping[Any, Any]]]:
    rows = db(
        r'''
        SELECT task_run_id,
               iv.start_time,
               end_time - iv.start_time AS duration,
               meta,
               label
          FROM dagon_intervals AS iv
          JOIN dagon_task_runs USING (task_run_id)
      ORDER BY task_run_id, iv.start_time
        ''',
        rid=rid,
    )
    items = {
        trun_id: [dict(zip(row.keys(), row)) for row in intervals]
        for trun_id, intervals in itertools.groupby(rows, key=lambda row: cast(TaskRunID, row[0]))
    }
    return items


def _gen_procs(db: Database, rid: RunID) -> Mapping[str, Any]:
    rows = db(
        r'''
        SELECT proc_exec_id,
               cmd,
               start_cwd,
               stdout,
               stderr,
               retc,
               dagon_proc_execs.start_time AS start_time,
               dagon_proc_execs.duration AS duration
            FROM dagon_proc_execs
            JOIN dagon_task_runs USING (task_run_id)
            WHERE run_id = :rid
        ''',
        rid=rid,
    )
    ret = {row[0]: dict(zip(row.keys(), row)) for row in rows}
    for _, row in ret.items():
        row.update({
            'stdout': base64.b64encode(row['stdout'] or b'').decode('ascii'),
            'stderr': base64.b64encode(row['stderr'] or b'').decode('ascii'),
        })
    return ret


def _generate_json_data(db: Database, rid: RunID) -> bytes:
    targets = _gen_targets(db, rid)
    intervals = _gen_intervals(db, rid)
    procs = _gen_procs(db, rid)
    start_time, = first(db(r'''SELECT time FROM dagon_runs WHERE run_id=:rid''', rid=rid))
    data = {
        'targets': targets,
        'start_time': start_time,
        'intervals': intervals,
        'proc_execs': procs,
    }
    buf = json.dumps(data).encode('utf-8')
    buf = buf.replace(b'\\', b'\\\\')
    buf = buf.replace(b'`', b'\\`')
    return buf


def generate(db: Database, rid: RunID) -> bytes:
    html = read_resource('inspect/tl.html')
    js = read_resource('inspect/tl.js')
    css = read_resource('inspect/tl.css')

    html = html.replace(b'[[TL_SCRIPT]]', js)
    html = html.replace(b'{{TL_CSS}}', css)
    html = html.replace(b'[[TL_DATA]]', _generate_json_data(db, rid))

    return html
