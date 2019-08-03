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
import logging
import time
from collections import deque
from pathlib import Path
from queue import Queue
from threading import Event
from typing import Union
from concurrent.futures import ThreadPoolExecutor

from logscrapertutorial.data.logfactory import create_fake_log, delete_data_files
from logscrapertutorial.lesson3 import coroutine

"""
First, let's make a thread that checks the files and sends their data to a thread safe Queue.
"""
@coroutine
def file_thread_sink(queue: Queue):
    while True:
        line = (yield)
        queue.put(line)


@coroutine
def file_line_generator(path: Union[str, Path]):
    with open(path) as file:
        while True:
            line = file.readline()
            logging.info(f'file_line_generator yield {line}')
            yield line


def file_reading_loop(coroutines, sink, end_event: Event):
    tasks = Queue()
    for coro in coroutines:
        tasks.put(coro)
    #  For some unknown reason this works as long as you don't observe it.
    #  The debugger never makes it to this next line.
    msg = f'deque size: {tasks.qsize()} end_event_state: {end_event.is_set()}'
    logging.info(msg)

    while not end_event.is_set():
        task = tasks.get()
        line = next(task)
        if line:
            sink.send(line)
        tasks.put(task)


def test_file_thread():
    delete_data_files()
    log1path = create_fake_log()
    log2path = create_fake_log()

    gen1 = file_line_generator(log1path)
    gen2 = file_line_generator(log2path)

    queue = Queue()
    end_event = Event()
    sink = file_thread_sink(queue)

    with ThreadPoolExecutor(max_workers=2) as executor:
        executor.submit(file_reading_loop, (gen1, gen2), sink, end_event)

        time.sleep(5)
        end_event.set()

    logging.info(f'Queue length is {queue.qsize()}')
