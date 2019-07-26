from collections import deque

from logscrapertutorial.data import fake
from logscrapertutorial.data.logfactory import create_fake_log, delete_data_files
from logscrapertutorial.utils import repeatfunc


def test_create_and_clean_up_logs():
    log_count = fake.pyint(max_value=5, min_value=1)
    gen = repeatfunc(create_fake_log, times=log_count)
    deque(gen, 0)
    assert delete_data_files() == log_count
