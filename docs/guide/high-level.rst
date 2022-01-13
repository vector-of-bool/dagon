High-Level Task Utility APIs
############################

Dagon ships with several high-level APIs that can be used within a task to
perform a wide variety of actions. A few of them are listed here to get you
started.


Manipulating Files - `dagon.fs`
*******************************

.. currentmodule:: dagon.fs

The `dagon.fs` module contains high-level APIs for asynchronously working with
the filesystem.

.. note::

    Internally, filesystem asynchrony is simulated using a thread pool to
    perform file system operations.

The following are a few of the most useful functions defined in `dagon.fs`:

- `copy_file()` - Copy a non-directory file from one location to another.
- `copy_tree()` - Copy an entire directory tree from one location to another.
- `remove()` - Delete files or directorys.
- `clear_directory()` - Delete the contents of directories.
- `safe_move_file()` - Move a file as if my relocating the hardlink, if
  possible. Otherwise performs a slow copy-then-delete.

The following are a few of the useful types defined in `dagon.fs`:

- `Pathish` - A type annotation that represents path parameters. This can be
  strings, `pathlib.Path` objects, or any type with an ``__fspath__`` method.
- `NPaths` - A type annotation that represents a parameter that can be a single
  `Pathish` or an iterable of `Pathish` objects. This allows a single function
  to transparently and type-safely handle either a single path or multiple
  paths. Give an `NPaths` object to `iter_pathish` to normalize to an iterable
  of `Pathish` objects.
- `IfExists` - Literal string type used to control behavior when a file or
  directory


Running Processes
*****************

.. currentmodule:: dagon.proc

The `dagon.proc` module implements APIs for running and controlling child
processes.

The following functions are a few of those defined in `dagon.proc`:

- `run` - Run a child process, waiting on the result. This function defines many
  knobs and switches, so read carefully!
- `spawn` - Like `run`, but returns a `RunningProcess` object without waiting
  for it to complete. The process can then be monitored and controlled by the
  caller.
- `define_cmd_task` - Define a `~dagon.task.Task` that simply executes a
  subprocess as-if via `run`.