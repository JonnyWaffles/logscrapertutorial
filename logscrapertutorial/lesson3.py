"""
########
Lesson 3
########

********************************
Expanding a data processing pipe
********************************
The previous lesson composed a functional data processing pipe.
This works great if we only need to do one thing with
the input, but that's never the case is it?

How can we organize sending different records to different pipes?

A word on generators
====================
Python's generators are awesome. Much has been written on them by individuals
far more experienced than I, but below is a quick summary. See `PEP 492 <https://www.python.org/dev/peps/pep-0492/`_
for details.

Generators produce a value. They're like functions, except when they ``yield`` they
preserve their current state.

What most people don't know is that ``yield`` is an expression, and generators can also
receive values via ``send(value)``. Additionally, Generators can use ``yield from``
to delegate control flow to another generator. If this sounds nuts just hang on,
the examples will help.

When generators receive values, they can be considered "legacy" co-routines.

Co-routine or generator?
========================
There are two distinct types, generators and co-routines.
Formally, co-routines are returned by an async def func().
**Note** the co-routine does not actually have to await anything.

Formal Co-routines cannot ``yield`` / ``yield from``, they can only ``await``.
Legacy "co-routine" generators can be forced in to compatibility with the
new formal co-routine when decorated with ``types.coroutine()``.

Native co-routines cannot be used standard iteration.

In this section we're going to start with using legacy generators as informal co-routines.
"""
from datetime import datetime, timedelta
from typing import Generator, Callable, Any

from logscrapertutorial.data.logfactory import FakeDictEntryFactory
from logscrapertutorial.lesson2 import pluck_state, get_metadata, pipe
from logscrapertutorial.utils import repeatfunc


def coroutine(func):
    """
    A decorator to create our coroutines from generator definitions.

    Calling a generator function does not start running the function!

    Instead, it returns a generator object. All co-routines must be "primed"
    by calling ``next`` or ``send` to run up until the first ``yield``.

    Args:
        func: A function with a yield
    """

    def start(*args, **kwargs):
        cr = func(*args, **kwargs)
        next(cr)
        return cr

    return start


"""
Remember our functional pipe? Similarly, we can chain coroutines together and push data through with ``send()``!

Let's pretend we want to make a data pipeline to publish all the log record states to a fancy API. We need a generator
to provide values to the pipe, co-routines to modify it, and then our fake API call will be the end point.
"""


def start(generator: Generator, target):
    """Given a data source and a target start the pipe!"""
    while True:
        try:
            item = next(generator)
            target.send(item)
        except (StopIteration, GeneratorExit):
            break


def log_data_generator(records=None):
    """An example generator for this demo."""
    if not records:
        records = repeatfunc(FakeDictEntryFactory, 10)
    for record in records:
        yield record


@coroutine
def state_plucking_coroutine(target):
    """The first coroutine in our data pipeline. Target is the next coroutine in the system.

    Args:
        target:
    """

    def filter_record(record):
        """An example of how we might use coroutines to filter data."""
        if isinstance(record, dict) and record.get('nestedmetadata'):
            target.send(pluck_state(record))

    while True:
        data = yield
        filter_record(data)


@coroutine
def highly_advanced_state_api():
    """Our demo "sink". Notice it has no target argument because this is the end
    of our data system.
    """
    while True:
        state = yield
        print(state)  # Highly advanced API!


def run():
    pipe = state_plucking_coroutine(highly_advanced_state_api())
    start(log_data_generator(), pipe)


def test_pipe():
    """Should print 10 states!"""
    run()


"""
Didn't we just recreate the functional pipe line? Yes. But consider how we can use this technique
to send data down various branching pipelines. Let's make a more complicated data flow. One where records
from yesterday go to one sink, and records from today to another. Also, let's abstract some of our generalized
functionality a bit.

.. note:: There's probably a much cuter way to write this API but I broke it out below for demonstration.
"""
@coroutine
def transformation(func: Callable[[Any], Any], target: Generator):
    """Transforms data and sends it down the pipe.

    Args:
        func: A function which transforms the data.
        target: Next processor
    """
    while True:
        data = yield
        target.send(func(data))


@coroutine
def coro_filter(func, target):
    """Conditionally sends data down the pipe.

    Args:
        func: A filter function which returns a boolean
        target: Next processor
    """
    while True:
        data = yield
        if func(data):
            target.send(data)


@coroutine
def sink(func: Callable[[Any], Any]):
    """The semantic end point for our system.

    .. note::

        Alternatively, we could just set coroutine targets to None
        and then optionally pass the data down the pipe. However, I like
        the explicitness of declaring a sink.

    Args:
        func: The last operation to peform against the data.
    """
    while True:
        data = yield
        func(data)


@coroutine
def broadcaster(*targets):
    """Broadcast the data to all the coroutines. Basically a routing station.

    There's probably a more formal name for this piece of the pipe. I need
    to do some more research

    Args:
        targets: A list of coroutines to send the data
    """
    while True:
        data = yield
        for target in targets:
            target.send(data)


"""
Using the above abstractions let's compose our two end points.
"""
def old_record_state_printing_flow():
    endpoint = sink(lambda state: print(f'Old record processor received {state}'))
    return state_plucking_coroutine(endpoint)


def todays_record_state_printing_flow():
    endpoint = sink(lambda state: print(f'Today\'s record processor received {state}'))
    return state_plucking_coroutine(endpoint)


"""
Now let's compose a date filter to use in our filter co-routine
"""
def get_date(metadata: dict):
    return metadata.get('datetime')


def is_old_datetime(datetime_instance: datetime):
    return datetime_instance.date() < datetime.now().date()


datetime_filter_func = pipe(get_metadata, get_date, is_old_datetime)

"""
Next compose our pieces together using a filter to direct records to different sinks!
"""
old_record_processor = coro_filter(datetime_filter_func,
                                   old_record_state_printing_flow())
today_or_after_record_processor = coro_filter(lambda d: not datetime_filter_func(d),
                                              todays_record_state_printing_flow())

datetime_broadcaster = broadcaster(old_record_processor, today_or_after_record_processor)


"""
We're now ready to send records to the broadcaster which will then direct the data down
a path based on the datetime metadata key.
"""
def test_old_pipe():
    """Prints hi from the old data sink
    """
    today = datetime.now()
    yesterday_datetime = today - timedelta(days=1)
    yesterdays_record = FakeDictEntryFactory(nestedmetadata__datetime=yesterday_datetime)

    early_record_gen = log_data_generator([yesterdays_record])
    start(early_record_gen, datetime_broadcaster)


def test_new_pipe():
    """Prints hi from the new data sink
    """
    today = datetime.now()
    todays_record = FakeDictEntryFactory(nestedmetadata__datetime=today)
    late_record_gen = log_data_generator([todays_record])
    start(late_record_gen, datetime_broadcaster)


def test_random_data_down_the_pipe():
    """Sends 100 random records down the pipe for demo purposes
    """
    records = [FakeDictEntryFactory() for _ in range(100)]
    record_gen = log_data_generator(records)
    start(record_gen, datetime_broadcaster)


"""
In this silly example all of these machinations may seem extreme. But imagine how we could
use many different generators many different pipes to flow data throughout a system, transform it, and process
it at various sinks.  
"""
