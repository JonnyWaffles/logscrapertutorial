"""
########
Lesson 2
########

****************************************
Making a functional data processing pipe
****************************************
let's presume for a moment we're okay with loading all the objects
to be processed in to memory. How would we then process each entry?
This is a silly example to illustrate a point.

Let's pretend we want to extract the state out of each entry. We can break the task down in to a
series of functions. Let's get crazy
"""
from typing import Dict, Callable, Any
from functools import reduce

from logscrapertutorial.data.logfactory import FakeDictEntryFactory
from logscrapertutorial.utils.enums import USStateEnum


def pipe(*funcs: Callable[[Any], Any]) -> Callable[[Any], Any]:
    """An example of a higher order function.
    Turns a series of funtions in to a pipe,
    where the return result of each function
    is passed in to the next one.

    Composite is the returned function that accepts a single value, X. It then reduces :attr:`funcs`
    by calling each function in order.

    Args:
        funcs: A sequence of single argument callables
    """
    def composite(x):
        def my_reducer(y, f):
            return f(y)
        return reduce(my_reducer, funcs, x)
    return composite


def get_metadata(obj: Dict) -> Dict:
    """First step is to unpack the nested metadata.

    Args:
        obj: The dictionary created from the json string.
    """
    return obj.get('nestedmetadata')


def get_state_from_meta(metadata: Dict) -> str:
    """Second step unpack the nested state field.

    Args:
        metadata:
    """
    return metadata.get('state')


def pluck_state(obj: Dict) -> str:
    """A wrapper to illustrate composing
    the above two functions.

    Args:
        obj: The dictionary created from the json string.
    """
    plucker = pipe(get_metadata, get_state_from_meta)
    return plucker(obj)


"""
You may ask why go through the trouble of composing a higher order function for something so simple?
Why not just write an imperative function like this.
"""


def imperative_pluck_state(obj: Dict) -> str:
    state = None
    try:
        metadata = obj['nestedmetadata']
        state = metadata['state']
    except KeyError:
        pass
    return state


"""
While in this silly example, it may make perfect sense to write an imperative function, consider the power
of being able to write and test individual functions and then compose them. Say we wanted to add functionality
to guarantee the text is upper case so we can then perform a transformation from code to name?
"""


def upper(text: str):
    return text.upper()


def state_abbrev_to_name(text: str):
    return USStateEnum[text].value


pluck_state_name = pipe(pluck_state, upper, state_abbrev_to_name)
"""
This time I didn't wrap the function and just created it directly with pipe.
See how expressive it is to compose functions this way?

Let's test what we've made so far.
"""


def test_pluck_state():
    data = FakeDictEntryFactory(nestedmetadata__state='VA')
    assert pluck_state(data) == 'VA'
    assert pluck_state_name(data) == 'Virginia'
