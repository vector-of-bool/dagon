import argparse
from pathlib import Path
import datetime
import enum
import sys
import textwrap
from typing import NoReturn, Sequence, Optional, Iterable
from string import Template
from typing_extensions import TypeAlias

from dagon.db import Database, FileInfo, RunID
from dagon.ui import ansi
from dagon.util import first, unused
from . import graph, timeline


class QueryChoice(enum.Enum):
    Run = 'run'
    AllRuns = 'all-runs'
    Proc = 'proc'
    Files = 'files'
    Graph = 'graph'
    Timeline = 'timeline'


_RUN_INFO_TEMPLATE = Template(r'''Run ID: $run_id
Start time: $start_time_iso ($start_time_human)
''')

_TASK_RUN_REPORT = Template(r'''Task: $task_name
  Start time: $start_time_iso ($start_time_human)
    Duration: ${duration} sec
      Result: $result
''')

_SUBPROC_EXEC_REPORT = Template(r'''External process: $exec_id
  Command: $process_cmdline
  Start Directory: $cwd
  Start time: $start_time_iso ($start_time_human)
  Duration: $duration sec
  Exit code: $retc
  stdout: $stdout_what
  stderr: $stderr_what
''')


def make_pretty_date(dt: datetime.datetime) -> str:
    # Python 3.7 adds a `fromisoformat`. Use that after upgrading.
    return dt.strftime('%A, %B %d, %I:%M:%S %p')


_RunTup: TypeAlias = 'tuple[RunID, float]'
_ProcTup: TypeAlias = 'tuple[int, int, str, str, str, str, int, int, float]'


def _print_run_info(row_tup: _RunTup) -> None:
    run_id, start_time_ = row_tup
    start_time = datetime.datetime.fromtimestamp(start_time_)

    report = _RUN_INFO_TEMPLATE.substitute(run_id=run_id,
                                           start_time_iso=start_time.isoformat(),
                                           start_time_human=make_pretty_date(start_time))
    print(report)


def _print_proc(proc_tup: _ProcTup, indent: str = '') -> None:
    exec_id, _task_run_id, cmd, cwd, stdout, stderr, retc, start_time_, duration = proc_tup
    unused(_task_run_id)
    stdout_what = f'{len(stdout)} bytes' if stdout is not None else 'Not attached'
    stderr_what = f'{len(stderr)} bytes' if stderr is not None else 'Not attached'
    start_time = datetime.datetime.fromtimestamp(start_time_)
    report = _SUBPROC_EXEC_REPORT.substitute(exec_id=exec_id,
                                             process_cmdline=cmd,
                                             cwd=cwd,
                                             start_time_iso=start_time.isoformat(),
                                             start_time_human=make_pretty_date(start_time),
                                             retc=retc,
                                             duration=duration,
                                             stdout_what=stdout_what,
                                             stderr_what=stderr_what)
    report = textwrap.indent(textwrap.dedent(report), indent)
    print(report)


def _print_proc_execs(_args: argparse.Namespace, db: Database, task_run_id: int, indent: str = '') -> None:
    execs = db('SELECT * FROM dagon_proc_execs WHERE task_run_id = :tri ORDER BY start_time', tri=task_run_id)
    for proc in execs:
        _print_proc(proc, indent)


def _run_query(runid: Optional[int], db: Database) -> Optional[_RunTup]:
    rows: Iterable[_RunTup] = db(
        r'''
        SELECT run_id, time
        FROM dagon_runs
        WHERE run_id=:run OR :run IS NULL
        ORDER BY run_id DESC
        ''',
        run=runid,
    )
    return first(rows, default=None)


def _print_run_query(args: argparse.Namespace, db: Database) -> int:
    run_tup = _run_query(args.run, db)
    if not run_tup:
        raise RuntimeError('Database has no runs')

    _print_run_info(run_tup)
    run_id = run_tup[0]

    rows = db('''SELECT task_run_id,
                   task_id,
                   run_id,
                   end_state,
                   start_time,
                   duration,
                   name
                FROM dagon_task_runs
                JOIN dagon_tasks USING(task_id)
                WHERE run_id=:rid
                ORDER BY duration DESC
            ''',
              rid=run_id)
    for task_run_id, _target_id, _run_id, end_state, start_time_, duration_s, task_name in rows:
        unused(_target_id, _run_id)
        color_fn = {
            'succeeded': ansi.bold_green,
            'failed': ansi.bold_red,
            'cancelled': ansi.bold_yellow,
            'pending': ansi.bold_cyan,
            'running': ansi.bold_cyan,
        }.get(end_state, lambda s: s)
        colored_res = color_fn(end_state.upper())
        start_time = datetime.datetime.fromtimestamp(start_time_)
        report = _TASK_RUN_REPORT.substitute(
            task_name=task_name,
            start_time_iso=start_time.isoformat(),
            start_time_human=make_pretty_date(start_time),
            duration=duration_s,
            result=colored_res,
        )
        print(report)
        _print_proc_execs(args, db, task_run_id, indent='  ')

    return 0


def _print_all_runs(_args: argparse.Namespace, db: Database) -> int:
    for row in db('SELECT * FROM dagon_runs'):
        _print_run_info(row)
    return 0


def _print_proc_output(args: argparse.Namespace, db: Database) -> int:
    if not args.proc_id:
        raise RuntimeError('Pass a --proc-id to query')
    rows = list(db('SELECT * FROM dagon_proc_execs WHERE proc_exec_id=:id', id=args.proc_id))
    if not len(rows):
        raise RuntimeError(f'No subprocess with the given ID {args.proc_id}')
    tup = rows[0]
    assert len(rows) == 1
    _print_proc(tup)
    stdout, stderr = tup[4], tup[5]
    if stdout is not None:
        print('--------------- stdout ---------------')
        print(stdout.decode(), end='')
    if stderr is not None:
        print('--------------- stderr ---------------')
        print(stderr.decode(), end='')
    return 0


def _extract_file(db: Database, file: FileInfo, dest: Path) -> None:
    datas = db.iter_file_data(file)
    dest.parent.mkdir(parents=True, exist_ok=True)
    with dest.open('wb') as fd:
        for blob in datas:
            fd.write(blob)


def _do_files(args: argparse.Namespace, db: Database) -> int:
    dest_dir = None
    ex_pat = args.extract

    if ex_pat:
        dest_dir = Path(args.dest or '.')
        dest_dir.mkdir(parents=True, exist_ok=True)

    run_tup = _run_query(args.run, db)
    if not run_tup:
        raise RuntimeError('Database contains no runs')
    run_id = run_tup[0]
    files = db.iter_files(run_id=run_id)

    if ex_pat is None:
        print(f'Run {run_id} contains the following files:')

    for info in files:
        if ex_pat is None:
            # We're just listing files, not extracting
            size = info.pretty_size if args.human else f'{info.size} bytes'
            print(f'  {info.path} ({size})')
            continue

        if not info.path.match(ex_pat):
            continue

        components = str(info.path).split('/')
        if args.strip_components >= len(components):
            print(f'WARN: Cannot extract {info.path} with --strip-components N >= {len(components)}')
            continue

        dest_relpath = '/'.join(components[args.strip_components:])
        assert dest_dir
        dest = dest_dir / dest_relpath
        print(f'Extract {info.path} to {dest} ... ', end='')
        _extract_file(db, info, dest)
        print('Done')

    return 0


def _do_graph(args: argparse.Namespace, db: Database) -> int:
    if not graph.HAVE_GRAPHVIZ:
        print('Install the Python `graphviz` module to use `graph` inspection', file=sys.stderr)
        return 2
    run_tup = _run_query(args.run, db)
    if not run_tup:
        print('Database contains no runs')
        return 1
    graph.generate_graph(db, run_tup[0])
    return 0


def _do_timeline(args: argparse.Namespace, db: Database) -> int:
    run_tup = _run_query(args.run, db)
    if not run_tup:
        print('Database has no Dagon runs')
        return 1
    html_content = timeline.generate(db, run_tup[0])
    Path(args.out or 'dagon-timeline.html').write_bytes(html_content)
    return 0


def _argparse_run(p: argparse.ArgumentParser) -> None:
    p.set_defaults(choice=QueryChoice.Run)


def _argparse_all_runs(p: argparse.ArgumentParser) -> None:
    p.set_defaults(choice=QueryChoice.AllRuns)


def _argparse_proc(p: argparse.ArgumentParser) -> None:
    p.set_defaults(choice=QueryChoice.Proc)


def _argparse_files(p: argparse.ArgumentParser) -> None:
    p.set_defaults(choice=QueryChoice.Files)
    p.add_argument(
        '--extract',
        '-e',
        nargs='?',
        const='*',
        help='Extract files (with optional glob pattern)',
    )
    p.add_argument('--dest', '-d', help='Destination directory for the files (used with --extract)')
    p.add_argument('--strip-components',
                   '-p',
                   help='Strip leading directory components from source (used with --extract)',
                   type=int,
                   default=0)


def _argparse_graph(p: argparse.ArgumentParser) -> None:
    p.set_defaults(choice=QueryChoice.Graph)


def _argparse_timeline(p: argparse.ArgumentParser) -> None:
    p.set_defaults(choice=QueryChoice.Timeline)
    p.add_argument('--out', '-o', help='Destination path for the HTML timeline')


def main(argv: Sequence[str]) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--database', help='The database to read [.dagon.db]')
    parser.add_argument('--run', help='The run to inspect (default is the latest)', type=int)
    parser.add_argument('--proc-id', help='A subprocess ID (for `proc` mode)', type=int)
    parser.add_argument('--human-readable',
                        '-H',
                        dest='human',
                        help='Prefer human-readable value representatoins',
                        action='store_true')

    sub = parser.add_subparsers()

    _argparse_run(sub.add_parser(QueryChoice.Run.value, help='Request runs'))
    _argparse_all_runs(sub.add_parser(QueryChoice.AllRuns.value, help='Information about all available runs'))
    _argparse_proc(sub.add_parser(QueryChoice.Proc.value, help='Process information'))
    _argparse_files(sub.add_parser(QueryChoice.Files.value, help='Embedded files'))
    _argparse_graph(sub.add_parser(QueryChoice.Graph.value, help='Generate a target dependency graph from a run'))
    _argparse_timeline(sub.add_parser(QueryChoice.Timeline.value, help='Generate an interactive execution timeline'))

    # parser.add_argument(
    #     'query', help='The attribute to query', choices=[v.value for v in QueryChoice])
    args = parser.parse_args(argv)

    db_path = Path(args.database or '.dagon.db').absolute()
    db = Database.open(db_path, mode='open-existing')

    ansi.ensure_ansi_term()
    if not hasattr(args, 'choice'):
        parser.print_usage()
        return 1

    query = QueryChoice(args.choice)
    fn = {
        QueryChoice.Run: _print_run_query,
        QueryChoice.AllRuns: _print_all_runs,
        QueryChoice.Proc: _print_proc_output,
        QueryChoice.Files: _do_files,
        QueryChoice.Graph: _do_graph,
        QueryChoice.Timeline: _do_timeline,
    }[query]
    return fn(args, db)


def start() -> NoReturn:
    sys.exit(main(sys.argv[1:]))
