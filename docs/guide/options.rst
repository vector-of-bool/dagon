Execution Options
#################

Dagon has a concept of "options," which are globally defined values shared
between all tasks for the duration of an execution. These are provided by the
invoker (user) up-front before task execution begins. They should be declared
globally by using `.option.add`. The option object returned should be stored and
used to access the option value within the task function using
`.option.value_of` or the `Option.get() <.option.Option.get>` method on the
returned object::

    from dagon import task, option

    out_dir: option.Option[str] = option.add('out-dir', str, doc='The output directory of the build')

    @task.define()
    async def build_native() -> None:
        await do_build(out_dir.get())

    @task.define()
    async def build_web() -> None:
        await do_web(out_dir.get())

Execution options may be provided at the command line:

.. code-block:: bash

    $ dagon build-native build-web -o out-dir=build-tmp/

The meaning of these options is up to you as the task author.

.. note::
    Names of options attached to a task graph must be unique. Adding the same
    option name twice will generate an error.

If you have an option with a limited set of valid values, you should use an
`~enum.Enum` type with ``str`` keys as your type. Dagon will list the valid
values for the option when the user enumerates the execution options::

    class BuildConfig(enum.Enum):
        Debug = 'debug'
        Release = 'release'

    config_opt = option.add('config', BuildConfig, doc='The build configuration')

    @task.define()
    async def build_thing() -> None:
        config: BuildConfig = config_opt.get()
        ...

.. code-block:: bash

    $ dagon --list-options
    config: BuildConfig {debug, release}
        The build configuration


Default Option Values
=====================

If an option is not provided by the user at the command line, asking for the
option within the task will return ``None``. When the option is declared, you
may provided a default value with the ``default`` keyword argument, which may
be either an immediate value or a function that will be lazily evaluated to
obtain the default value::

    optoin.add('name', str, default=lambda: calc_default_name(42))

The function semantics are useful if the default value might be costly to
compute.

.. note::
    Evaluation of the default values occurs before task execution, not when the
    user asks for a value.


Secondary Validation
====================

Beyond simply specifying a type, you can add additional option validation using
the ``validate`` keyword argument. It accepts a callable that should take a
value of the options type and return either ``None`` in case of success, or
a string that indicates the error::

    def _is_positive(i: int) -> str | None:
        if i < 0:
            return 'Value must be a positive integer'
        return None

    option.add('age', int, validate=_is_positive)


Custom Option Types
===================

Dagon has support for several built-in types, standard library types, and any
enum class. To provide your own type, add a ``__dagon_parse_opt__`` static
method to your class. This function should accept a single string and return
an instance of your custom class::

    class MyCustomType:
        @staticmethod
        def __dagon_parse_opt__(opt: str) -> MyCustomType:
            return MyCustomType(opt, 'joe', 42)
        ...

    some_opt = option.add('thing', MyCustomType)