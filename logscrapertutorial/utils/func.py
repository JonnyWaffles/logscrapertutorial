from collections import deque
from itertools import starmap, repeat


def repeatfunc(func, times=None, *args):
    """Repeat calls to func with specified arguments.

    Example:  repeatfunc(random.random)
    """
    if times is None:
        return starmap(func, repeat(args))
    return starmap(func, repeat(args, times))


def exhaust(generator):
    """Exausts an iterable. Shortcut to deque with maxlen 0.

    As I understand it deque is the most Pythonic way to exhaust an iterable.
    """
    deque(generator, maxlen=0)
