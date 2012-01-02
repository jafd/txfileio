from twisted.internet import defer, threads
from twisted.python.failure import Failure
import Queue
import sys
import time

class FileIOProxy(object):
    """
    A FileIOProxy proxies calls to file operations, so they all return Deferreds; the operations
    themselves are queued up and delegated to worker threads. Deferreds fire when the workers have
    finished the operations.
    """
    def __init__(self, manager, fd):
        self.manager = manager
        self.fd = fd

    def _proxyfunc(self, op):
        def _inner(*args, **kwargs):
            return self.manager.enqueue(op, self, args, kwargs)
        fn = getattr(self.fd, op)
        _inner.__name__ = fn.__name__
        _inner.__doc__ = fn.__doc__
        if hasattr(fn, '__dict__'):
            _inner.__dict__.update(fn.__dict__)
        return _inner

    def __getattr__(self, attrname):
        if hasattr(self.fd, attrname):
            if callable(getattr(self.fd, attrname)):
                return self._proxyfunc(attrname)
            return getattr(self.fd, attrname)
        raise AttributeError("The object of class '{0}' has no attribute '{1}'".format(self.fd.__class__.__name__, attrname))

class Operation(object):
    __slots__ = ('fd', 'name', 'deferred', 'args', 'kwargs', 'state')
    def __init__(self, **kwargs):
        self.args = kwargs.get('args', None)
        self.kwargs = kwargs.get('kwargs', None)
        self.state = 'new'
        for i in ('fd', 'name', 'deferred'):
            setattr(self, i, kwargs.get(i))
            
    def __str__(self):
        return "File operation: {0}({1}, {2}), state = {3}".format(self.name, repr(self.args), repr(self.kwargs), self.state)

class Runner(object):
    __slots__ = ('fd', 'busy', 'running', 'queue', 'manager')
    def __init__(self, manager):
        self.queue = Queue.Queue()
        self.fd = None
        self.busy = False
        self.running = False
        self.manager = manager

    def enqueue(self, op):
        self.queue.put(op)
        return op.deferred
        
    def __call__(self):
        self.running = True
        while self.running:
            try:
                op = self.queue.get(True, self.manager.queue_timeout)
                self.fd = op.fd
                op.state = 'running'
                self.busy = True
                try:
                    if (op.fd is None) and (op.name != 'open'):
                        raise RuntimeError("Calling a file operation {0} on None".format(op.name))
                    if op.name == 'open':
                        result = self.manager.take(open(*op.args, **op.kwargs))
                        op.fd = result
                    else:
                        result = getattr(op.fd.fd, op.name)(*op.args, **op.kwargs)
                    op.state = 'success'
                    threads.blockingCallFromThread(self.manager.reactor, op.deferred.callback, result)
                except Exception as e:
                    op.state = 'failure'
                    threads.blockingCallFromThread(self.manager.reactor, op.deferred.errback, e)
            except Queue.Empty:
                pass
        return True

class FileIOManager(object):
    """
    This class is used to process file I/O. It spawns runners into separate threads,
    and those runners fetch I/O operations from the I/O queue.
    """
    def __init__(self, reactor, maxrunners=5, queue_timeout=1):
        self.reactor = reactor
        self.accept_operations = True
        self.queue_timeout = queue_timeout
        self.runners = []
        self.runner_no = 0
        self.stopdeferred = defer.Deferred()
        self.maxrunners = maxrunners
        for i in range(self.maxrunners - 1):
            self.spawnRunner()

    def buildOpQueue(self):
        return Queue.Queue()

    def take(self, fileobject):
        """
        Add an existing file-like object to be processed. Returned is a FileIOProxy
        """
        fp = FileIOProxy(self, fileobject)
        return fp

    def enqueue(self, op, fd, args, kwargs):
        if not self.accept_operations:
            raise RuntimeError("Operations queued past service stop")
        d = defer.Deferred()
        operation = Operation(name=op, args=args, kwargs=kwargs, deferred=d, fd=fd)
        filtered = filter(lambda x: x.fd is fd, self.runners)
        if len(filtered):
            filtered[0].enqueue(operation)
        else:
            self.runner_no = (self.runner_no + 1) % len(self.runners)
            self.runners[self.runner_no].enqueue(operation)
        return d

    def spawnRunner(self):
        r = Runner(self)
        self.runners.append(r)
        return threads.deferToThread(r).addCallback(lambda _: self.runners.remove(r))

    def stop(self):
        self.accept_operations = False
        for x in self.runners:
            x.running = False
        
    def open(self, *args, **kwargs):
        return self.enqueue('open', None, args, kwargs)
