"""
Makes fake logs for our project using factory boy.

Factoryboy is a super fun API to easily instantiate realistic fake data.
You don't need to understand how it works for this tutorial.
"""
import json
from collections import deque
from functools import partial
from pathlib import Path
from typing import Union
from uuid import uuid4

import factory

from logscrapertutorial.data import DATA_DIRECTORY, fake
from logscrapertutorial.utils import repeatfunc


class FakeNestedMetaDataFactory(factory.DictFactory):
    datetime = factory.Faker('date_this_month')
    uuid = factory.Faker('uuid4')
    state = factory.Faker('state_abbr')


class FakeDictEntryFactory(factory.DictFactory):
    resource = factory.Faker('uri')
    user = factory.Faker('user_name')
    nestedmetadata = factory.SubFactory(FakeNestedMetaDataFactory)


# noinspection PyMethodParameters
class FakeLogRecordFactory(FakeDictEntryFactory):
    @factory.PostGeneration
    def write_log_record(obj: dict, create, extracted, **kwargs):
        filename = Path(extracted) if extracted else make_file_name_path()
        path: Path = DATA_DIRECTORY / filename
        if path.exists():
            with path.open('a') as file:
                jsonstr = json.dumps(obj)
                file.write(f'{jsonstr}\n')


def make_file_name_path():
    """Creates a random file name."""
    return Path(f'{uuid4().hex}.blob')


def create_fake_log(filename: Union[str, Path] = None, number_of_rows: int = None):
    """Creates a fake log in the data directory whose name is :attr:`filename`
    with a row count of :attr:`number_of_rows`.

    Args:
        filename: Name of the file. If not provided a random name will be created.
        number_of_rows: Number of json row strings to add to the file.

    .. note:: If the file already exists it will be appended.
    """
    if number_of_rows is None:
        number_of_rows = fake.pyint(min_value=0, max_value=100)

    part = Path(filename) if filename else make_file_name_path()
    path: Path = DATA_DIRECTORY / part
    path.touch()

    factoryfunc = partial(FakeLogRecordFactory, write_log_record=path)
    entry_generator = repeatfunc(factoryfunc, number_of_rows)
    deque(entry_generator, maxlen=0)


def delete_data_files() -> int:
    """Deletes all the fake files returning the deleted count."""
    to_remove = []
    for path in DATA_DIRECTORY.iterdir():
        if path.is_file() and path.suffix != '.py':
            to_remove.append(path)
    delete_all = map(lambda p: p.unlink(), to_remove)
    deque(delete_all, maxlen=0)
    return len(to_remove)
