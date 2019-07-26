"""
########
Lesson 1
########

****************
Simple Data Load
****************
When a new developer begins the json log reading project, the first step will be to
read the data in using  json.

Path is a super useful API to ease dealing with Paths. Don't use strings!

Let's imagine our target file is in the :attr:`~logscrapertutorial.data.DATA_DIRECTORY` directory.
Using our fake data generation tools we can create
a single record target file and access it with the provided name using the :mod:`pathlib`
:class:`pathlib.Path` objects.
"""
from pathlib import Path

from logscrapertutorial.data import DATA_DIRECTORY
from logscrapertutorial.data.logfactory import create_fake_log, delete_data_files

# Make sure our experimental data directory is clean of any data files
delete_data_files()

filename = 'part1.blob'
create_fake_log(filename, number_of_rows=1)

# Now that our fake log exists let's access it with Path
path = DATA_DIRECTORY / Path(filename)
assert path.exists()

"""
Our first step will be to load the data via json
"""
import json
data = json.loads(path.read_text())
assert isinstance(data, dict)

"""
This works, but what happens if there are two objects in the file?
"""
delete_data_files()
create_fake_log(filename, number_of_rows=3)
try:
    json.loads(path.read_text())
except json.decoder.JSONDecodeError:
    print('We cannot load the file!')

"""
We need to read the file line by line to load each object.
Sadly, there's no readline funtionality offered by the Path abstraction so we'll need to
handle it the old fashioned way.
"""
decoded_dictionaries = []
with open(path) as filename:
    for line in filename.readlines():
        decoded_dictionaries.append(json.loads(line))

assert len(decoded_dictionaries) == 3

"""
This method works, but it is pretty verbose. We can cut down our code footprint by using list comprehensions
and splitting the string on new line characters.
Read all the text
"""
text_content = path.read_text()
decoded_dictionaries = [json.loads(line) for line in text_content.split('\n') if line]
assert len(decoded_dictionaries) == 3

""""
That works, but the approach is suboptimal. Consider, we must load the entire text file
in to memory. What if we wanted to process each object at time? What if the text file
was continuously being appended by another process? Head over to lesson 2 where we get crazy.
"""
delete_data_files()
