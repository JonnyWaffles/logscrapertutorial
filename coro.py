
def coroutine(func):
    def start(*args, **kwargs):
        cr = func(*args, **kwargs)
        next(cr)
        return cr
    return start


@coroutine
def corogen(next_process=None):
    print('priming')
    x = None
    while True:
        print(x)
        _ = (yield from next_process) if next_process else (yield x)
        if _:
            x = _


@coroutine
def corogen2():
    x = yield
    while True:
        print(f'hi from coro 2: {x}')
        _ = yield
        if _:
            x = _


mycoro = corogen(corogen2())
