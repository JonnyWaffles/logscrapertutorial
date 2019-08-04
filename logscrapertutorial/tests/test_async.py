import asyncio
import logging
import time
from asyncio import QueueEmpty
from logscrapertutorial.data import fake
from queue import Queue, Empty
from threading import Thread, Event
from concurrent.futures import ThreadPoolExecutor


async def async_generator(name: str):
    """Creates fake messages until the program ends.

    Args:
        name: The name of the generator so we can demo two working at once.
    """
    while True:
        msg = f'{name}: {fake.word()}'
        yield msg
        #  This part is interesting. Yield does not imply an await!
        #  We must specifically await to return control back  to the loop.
        #  Sleep 0 is a super fun undocumented feature which just returns control
        #  but does  not incur the overhead of actually waiting for a set of time.
        await asyncio.sleep(0)


async def async_generator_reader(generator):
    """Pretend this is our super slick API.

    This function will continuously handle messages from the async generator.
    """
    async for msg in generator:
        print(msg)


# noinspection PyTypeChecker
async def run_reader_loop_for_time(seconds: int):
    """Binds two pipes together and runs them on the event loop for :attr:`seconds`.

    Args:
        seconds:
    """
    reader1 = async_generator_reader(async_generator('first'))
    reader2 = async_generator_reader(async_generator('second'))
    asyncio.create_task(reader1)
    asyncio.create_task(reader2)
    await asyncio.sleep(seconds)


def test_async_for():
    """This tests floods stdout with messages from our fake producer/consumers"""
    asyncio.run(run_reader_loop_for_time(2))


def thread_producer(name: str, queue: Queue, event: Event):
    """Produces messages until the kill event is triggered.

    Args:
        name: The name of our thread
        queue: The queue to add messages
        event: The kill signal
    """
    while True and not event.is_set():
        msg = f'{name} Thread: {fake.word()}'
        queue.put(msg)
        time.sleep(0.01)


async def threadsafe_async_print(name: str, queue: Queue, event: Event):
    """Function to test printing a thread Queue

    Args:
        name: Name of our thread
        queue: The queue to send the messages
        event: The kill signal
    """
    while True and not queue.empty() or event.is_set():
        item = queue.get()
        print(f"name: {name} received: {item}")
        await asyncio.sleep(0)


async def threadsafe_async_pipe(queue: Queue,  async_queue: asyncio.Queue, event: Event):
    """Checks the formal threadsafe queue for a message
    then places it on the async_queue

    Args:
        queue:
        async_queue:
        event: The kill event
    """
    while True and not event.is_set():
        try:
            item = queue.get(block=False)
            await async_queue.put(item)
            logging.debug(f"threadsafe_async_pipe put: {item}")
        except Empty:
            pass
        finally:
            await asyncio.sleep(0)


async def async_queue_reader(async_queue: asyncio.Queue, event: Event):
    """Checks the async queue for a message and if it exists prints it

    Think of this as our sink.

    Args:
        async_queue:
        event: Kill signal
    """
    while True and not event.is_set():
        try:
            """
            This part was hard to get right.
            First, you don't await get_nowait() unlike get(),
            I guess this is because no wait assumes the value is immediately available or throws.
            """
            msg = async_queue.get_nowait()
            logging.debug(f"async_queue_reader received: {msg}")
            async_queue.task_done()
        except QueueEmpty as err:
            """
            If our Queue is empty go back to sleep and check the event again.
            """
            pass
        finally:
            """
            We always want to sleep so the shut down event is continuously checked.
            """
            await asyncio.sleep(0)
    logging.debug('async_queue_reader shutting down!')
    assert async_queue.empty()  # I am curious if this always passes.


def async_consuming_thread(queue: Queue, end_event:  Event):
    """

    Args:
        queue: The thread safe Queue
        end_event: The kill signal
    """
    # Originally, I made a super interesting bug by
    # created the asyncio.Queue() here, which
    # for some unknown implementation reason caused the below
    # add_tasks coroutine to never run and silently be skipped.
    async def add_tasks():
        logging.debug('Async Consuming Thread Add Tasks Entered')
        async_queue = asyncio.Queue()  # The queue MUST be defined here prior to asyncio.run() being called.
        task1 = asyncio.create_task(threadsafe_async_pipe(queue, async_queue, end_event))
        task2 = asyncio.create_task(async_queue_reader(async_queue, end_event))
        await asyncio.gather(task1, task2)
        logging.debug('Async consuming add tests done!')

    asyncio.run(add_tasks())


def _test_asyncio_from_other_thread(queue, event):
    """
    Tests to see if we can run a loop from another thread
    """
    async def add_tasks():
        task1 = asyncio.create_task(threadsafe_async_print('first', queue, event))
        task2 = asyncio.create_task(threadsafe_async_print('second', queue, event))
        await asyncio.gather(task1, task2)

    asyncio.run(add_tasks())


def test_asyncio_from_other_thread():
    """The actual test case for spinning up :func:`_test_asyncio_from_other_thread`
    in another thread.
    """
    queue = Queue()
    event = Event()
    for item in range(10):
        queue.put(item)

    with ThreadPoolExecutor(max_workers=1) as executor:
        executor.submit(_test_asyncio_from_other_thread, queue, event)

    assert queue.qsize() == 0


def test_thread_producer():
    queue = Queue()
    end_event = Event()
    with ThreadPoolExecutor(max_workers=1) as executor:
        executor.submit(thread_producer, 'firstthread', queue, end_event)

        time.sleep(5)
        end_event.set()
    logging.log(logging.DEBUG, f'queue size is {queue.qsize()}')
    assert not queue.empty()


def test_multithreaded_async():
    """
    Let's get crazy. We're going to send messages from a thread to an event loop on another thread.
    """
    queue = Queue()
    end_event = Event()

    with ThreadPoolExecutor(max_workers=3) as executor:
        executor.submit(thread_producer, 'firstthread', queue, end_event)
        executor.submit(async_consuming_thread, queue, end_event)

        time.sleep(10)
        end_event.set()
        logging.info('End Event Triggered!')
