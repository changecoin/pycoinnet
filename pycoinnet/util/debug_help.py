
import asyncio
import logging
import sys

asyncio_Task = asyncio.Task

def _done_callback(f):
    try:
        r = f.result()
    except Exception as ex:
        logging.exception("task raised exception")
        print("exception!! =>", ex)
        import pdb; pdb.set_trace()
        #sys.excepthook(ex)

def Task(*args, **kwargs):
    f = asyncio_Task(*args, **kwargs)
    f.add_done_callback(_done_callback)
    return f

asyncio.Task = Task
