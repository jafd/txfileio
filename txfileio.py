from twisted.internet import defer

class FileIOProxy(object):
    """
    A FileIOProxy proxies calls to file operations, so they all return Deferreds.
    """
    def __init__(self, manager, fd):
        pass

    def _proxyfunc(self, op):
        def _inner(self, *args, **kwargs):
            return self.manager.enqueue(self, op, args, kwargs)
        fn = getattr(self.fd, op)
        _inner.__name__ = fn.__name__
        _inner.__doc__ = fn.__doc__
        _inner.__dict__.update(fn.__dict__)
        return _inner

    def __getattr__(self, attrname):
        if hasattr(self.fd, attrname) and callable(getattr(self.fd, attrname)):
            return self._proxyfunc(attrname)
        return object.__getattr__(attrname)


class FileIOManager(object):
    """
    A process which runs in a separate thread and does actually take care of I/O
    operations. It processes requests to open files, to read, write and seek in them.
    """
    def __init__(self, reactor, minrunners=2):
        self.reactor = reactor
        self.opqueue = self.buildOpQueue()
        self.resultqueue = self.buildResultQueue()
        self.accept_operations = True
        self.running = False
        self.runners = []
        self.stopdeferred = defer.Deferred()
        self.maxrunners = maxrunners

    def buildOpQueue(self):
        return Queue.Queue()

    def buildResultQueue(self):
        return Queue.Queue()

    def open(self, filename, mode="r", buffering=None):
        """
        Open a file. Return a Deferred which results in a filehandle proxy.
        The filehandle proxy is almost entirely like a file-like object, except
        t
        """
        pass

    def take(self, fileobject):
        """
        Add an existing file-like object to be processed. Returned is a FileIOProxy
        """
        fp = FileIOProxy(self, fileobject)
        return fp

    def enqueue(self, fd, op, args, kwargs):
        if not self.accept_operations:
            raise RuntimeError("Operations queued past service stop")
        # Clean up stopped runners
        self.runners = filter(lambda r: not r['stopped'], self.runners)
        # Find if we have non-busy runners
        ready = len(filter(lambda r: not r['busy'], self.runners)) > 0
        if not ready and len(self.runners) < self.maxrunners:
            self.spawnRunner()
        d = defer.Deferred()
        self.opqueue.put({
            'op' : op,
            'args' : args,
            'kwargs' : kwargs,
            'deferred' : d,
            'fd' : fd
            })
        return d

    def spawnRunner(self):
        def _runner(self, record):
            timeouts = 0
            while record['running']:
                try:
                    op = self.opqueue.get(block=True, timeout=1)
                    record['busy'] = True
                    record['currentfd'] = op['fd']
                    try:
                        if op['name'] == 'open':
                            result = open(*op['args'], **op['kwargs'])
                        else:
                            result = getattr(op['fd'], op['name']).__call__(*op['args'], **op['kwargs'])
                        self.reactor.callFromThread(op['deferred'].callBack, result)
                    except Exception as e:
                        pass
                except Queue.Empty:
                    timeouts += 1
                finally:
                    if timeouts > 10:
                        record['running'] = False
                return True

        def _recstop(self, record, result):
            self.runners[record]['stopped'] = True
            return result

        record = { 'running' : True, 'busy' : False, 'stopped' : False, 'currentfd' : None }
        self.runners.append(record)
        record['stopDeferred'] = self.reactor.deferToThread(_runner, self, record).addCallback(_recstop, self, record)

    def stop(self):
        self.accept_operations = False
        self.running = False
        pass
