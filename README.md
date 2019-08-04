# Purpose
A coworker wanted to learn Python to perform some log monitoring. Monitoring logs
felt like a great beginners project, as one could accomplish it as basic or as complex
as desired. As such, I decided to put together a little tutorial repository to both help
teach beginners, and inform myself of some of the more complex aspects of Python.

I hope others could benefit from this tutorial.

## Lessons
Each lesson takes the task a little bit further and explains why and how the code is designed the way it is.

1. Lesson 1 - Details how to read the json lines
2. Lesson 2 - Shows to how compose functions to process the data
3. Lesson 3 - Details how we can expand the functional pipe concept to create processing system from
legacy co-routines.
4. Lesson 4 - This is where we completely go off the rails and pass data from a dedicated I/O thread to
an asyncio event loop for processing

## Data
Fake data for the lessons will be generated in the data package.

## Status
Lessons 1-4 are in a good first draft

## Instructions
Clone down the repository and install the requirements with
`pip install -r requirements.txt`
Run the lessons and inspect the source! I recommend using
Pycharm with pytest support enabled so you can 
click run on each example as you go through the lessons.

## Credits
Shoutout to David Beazley and his [presentation](http://www.dabeaz.com/coroutines/Coroutines.pdf)
for inspiring me to work this article just to understand his work!
