import time
import database

client = database.Client()


def time_evaluation(func):
    def timed(*args, **kw):
        metadata = {}
        if 'metadata' in kw:
            if kw['metadata']:
                metadata = kw['metadata']
            del kw['metadata']
        ts = time.time()
        result = func(*args, **kw)
        te = time.time()
        metadata['result'] = result
        client.write_evaluation_data(method=func.__name__, driver=args[0].__module__.split('.')[-2],
                                     value=te-ts, metadata=str(metadata))
        return result
    return timed
