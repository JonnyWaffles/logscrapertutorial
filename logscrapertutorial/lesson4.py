"""
########
Lesson 4
########

********************************
Combining async and threads because we can
********************************
The previous lessons showed how we can use "generator based co-routines"
to build data pipes.

Organizing the data flow in to pipes helps us understand the system, and may
give the illusion of asynchronously processing, however each piece of data
is actually processed synchronously.

This lesson will teach how we can get crazy and process the data asynchronously.

Threads and asyncio
===================
Concurrency is a vast field. I highly recommend `This Real Python <https://realpython.com/python-concurrency/>`_
article. Long story short, Python's `GIl <https://realpython.com/python-gil/>`_ means threading incurs an overhead.
Asyncio uses a single threaded event loop, so it does not incur the wrath of the GIL, which makes
 it perfect for networking. However, there is
no way to asynchronously perform system file I/O (even in JavaScript file IO is actually wrapped in threads).

What does all this mean? We will use thread(s) to read files and async io to process their data.
All of this is of course overkill for our silly example but meant to show how we could scale it so networking
calls to various clients don't hold up each other!
"""
import asyncio
import logging
import time
from asyncio import QueueEmpty
from pathlib import Path
from queue import Queue, Empty
from threading import Event
from typing import Union
from concurrent.futures import ThreadPoolExecutor

from logscrapertutorial.data import fake
from logscrapertutorial.data.logfactory import create_fake_log, delete_data_files
from logscrapertutorial.lesson3 import coroutine

"""
First, let's make a thread that checks the files and sends their data to a thread safe Queue.
"""
@coroutine
def queue_sink(queue: Queue):
    while True:
        data = (yield)
        queue.put(data)


@coroutine
def file_line_generator(path: Union[str, Path]):
    with open(path) as file:
        while True:
            line = file.readline()
            # Once again for some unknown reason, we can view the
            # yield mesage when pytest logs this line but not the
            # actual {line} content, however it does appear in the queue.
            logging.info(f'file_line_generator yielding \n {line}')
            yield line


def file_reading_loop(coroutines, sink, end_event: Event):
    # Originally I tried using a deque object but the thread got cancelled
    # and never made it to the msg or the loop.
    # No idea why, but using a Queue instead seems to work!
    tasks = Queue()
    for coro in coroutines:
        tasks.put(coro)
    #  For some unknown reason this works as long as you don't observe it.
    #  The debugger never makes it to this next line if we use a break point.
    #  But if we read the queue after the thread runs the data is there!?
    msg = f'deque size: {tasks.qsize()} end_event_state: {end_event.is_set()}'
    logging.info(msg)

    while not end_event.is_set():
        task = tasks.get()
        line = next(task)
        if line:
            # Once again for an unknown reason we cannot view this log message in pytest cli
            # But we know when the thread ends the Queue has the data.
            logging.info(f'Found line!  {line}')
            sink.send(line)
        tasks.put(task)


def test_file_thread():
    """
    This test is odd. We know it works when we
    examine the Queue after the thread runs, however
    when we try to debug or inspect the code during runtime
    we cannot view the values or hit break points. I'll
    need to investigate this further, but we know it works
    because the queue is full of records!
    """
    delete_data_files()
    log1path = create_fake_log()
    log2path = create_fake_log()

    gen1 = file_line_generator(log1path)
    gen2 = file_line_generator(log2path)

    queue = Queue()
    end_event = Event()
    sink = queue_sink(queue)

    with ThreadPoolExecutor(max_workers=2) as executor:
        executor.submit(file_reading_loop, (gen1, gen2), sink, end_event)

        time.sleep(5)
        end_event.set()

    assert not queue.empty()

    for _ in range(queue.qsize()):
        logging.info(f'{queue.get()}')
    delete_data_files()


"""
We provided we can use coroutines to continuously read our files and send their data to a
queue.

Now, we'll build a new async thread running a routine which lands the Queued data
in to an async queue so our various fake APIs can process it.
"""


async def threadsafe_async_pipe(queue: Queue,  async_queue: asyncio.Queue, event: Event):
    """This is our first modern formal async co-routine. Notice the async declaration.
    This means this function can only be run within an event loop.

    The routine checks the formal threadsafe queue for a message
    then places it on thread unsafe async_queue, where various
    async routines can access it.

    Args:
        queue:
        async_queue:
        event: The kill event
    """
    while not event.is_set():
        try:
            #  Note we must explicitly not block.
            #  If the queue throws empty go back to sleep
            #  and check later.
            item = queue.get(block=False)
            await async_queue.put(item)
        except Empty:
            pass
        finally:
            await asyncio.sleep(0)


async def async_broadcaster(subscribers, async_queue: asyncio.Queue, end_event: Event):
    """The broadcaster checks the async queue for messages and then sends them to all
    subscribers. You could imagine this could be more complicated with multiple queues, topics,
    filters, routes, etc, but this demonstrates the point.

    Args:
        subscribers: All the listeners whom will register their handles on the event loop
            when they receive data
        async_queue: The thread unsafe async queue with all the data
        end_event: The thread safe termination signal
    """
    while not end_event.is_set():
        try:
            data = async_queue.get_nowait()
            for sub in subscribers:
                sub.send(data)
            async_queue.task_done()
        except QueueEmpty as err:
            pass
        finally:
            await asyncio.sleep(0)
    logging.info(
        'async broadcaster shutting down.\n'
        f'async queue is empty: {async_queue.empty()}'
    )


@coroutine
def subscriber(end_point):
    """Wrapper to subscribe to a broadcaster and then schedule
    the handler :attr:`end_point` to run on the loop.

    Args:
        end_point: An async callback to handle the data
    """
    while True:
        data = (yield)
        asyncio.create_task(end_point(data))


"""
Let's test what we have so far!
"""


def test_single_thread_publish_subscribe():
    sink_list = []

    async def logger_endpoint(data):
        """A fake end point to simulate
        network processing time or some other
        async operation.
        """
        await asyncio.sleep(1)
        logging.info(f'End point received: {data}')
        sink_list.append(data)

    queue = Queue()
    for _ in range(20):
        queue.put(fake.words())
    end_event = Event()

    async def add_tasks(runtime: int):
        """
        Args:
            runtime: The time the app should run before we kill it
        """
        async_queue = asyncio.Queue()
        #  Create our coroutines
        print_api_subscription = subscriber(logger_endpoint)
        producing_coro = threadsafe_async_pipe(queue, async_queue, end_event)
        broadcasting_coro = async_broadcaster([print_api_subscription], async_queue, end_event)
        #  Schedule them on the loop as tasks
        producing_task = asyncio.create_task(producing_coro)
        broadcasting_task = asyncio.create_task(broadcasting_coro)

        await asyncio.sleep(runtime)
        end_event.set()
        logging.info('End Event triggered awaiting shutdown.')
        await asyncio.gather(producing_task, broadcasting_task)
        logging.info('Shut down complete')

    asyncio.run(add_tasks(1))
    assert sink_list


"""
As you can see our single threaded example works. Now all we need to is put it all together.
Run one thread that continuously reads files adding data to the thread safe queue,
and another thread where our event loop processes the data.
"""

