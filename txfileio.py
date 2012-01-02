from twisted.internet import defer, threads
from twisted.python.failure import Failure
import Queue
import sys
import time

class FileIOProxy(object):
    """
    A FileIOProxy proxies calls to file operations, so they all return Deferreds; the operations
    themselves are queued up and delegated to worker threads. Deferred fires when the worker has
    finished the operation.
    """
    def __init__(self, manager, fd):
        """
        @param manager: A FileIOManager instance
        @param fd: A file-like object
        
        The class constructor is not normally called directly. Use manager.open() to get a FileIOProxy
        object. If you have a ready-made object which you want to treat in an asynchronous manner,
        call manager.take(fd), and it will give a FileIOProxy object wrapped around your object.
        """
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

    def runInteraction(self, proc, *args, **kwargs):
        """
        @param proc: a callable which is passed the real file-like object
        @param *args: additional positional arguments passed to the said callable
        @param **kwargs: additional keyword arguments passed to the said callable
        
        @return: a Deferred which fires with whatever the callable returns
        
        It may be useful to delegate a bunch of small I/O operations to be executed in a batch,
        in regular blocking fashion - mostly because it would be faster.
        """
        return self.manager.enqueue('interaction', self, args, kwargs, proc=proc)

    def __getattr__(self, attrname):
        if hasattr(self.fd, attrname):
            if callable(getattr(self.fd, attrname)):
                return self._proxyfunc(attrname)
            return getattr(self.fd, attrname)
        raise AttributeError("The object of class '{0}' has no attribute '{1}'".format(self.fd.__class__.__name__, attrname))

class Operation(object):
    """
    The Operation objects are tossed around from FileIOProxies to FileIOManagers and to Runners.
    Operation's name, arguments, and file-like object reference are all stored here.
    
    Normally, none needs to instantiate an Operation in his/her own code.
    """
    __slots__ = ('fd', 'name', 'deferred', 'args', 'kwargs', 'state', 'callable')
    def __init__(self, **kwargs):
        self.state = 'new'
        for i in ('fd', 'name', 'deferred', 'args', 'kwargs', 'callable'):
            setattr(self, i, kwargs.get(i))
            
    def __str__(self):
        return "File operation: {0}({1}, {2}), state = {3}".format(self.name, repr(self.args), repr(self.kwargs), self.state)

class Runner(object):
    """
    The Runner is a callable object which actually executes I/O operations
    and interactions. It is never used directly.
    """
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
        
    def execute(self, op):
        """
        @param op: operation to execute
        @return: None
        
        
        """
        self.fd = op.fd
        op.state = 'running'
        self.busy = True
        try:
            if (op.fd is None) and (op.name not in ('open', '_stop')):
                raise RuntimeError("Calling a file operation {0} on None".format(op.name))
            if op.name == '_stop':
                self.running = False
                result = True
            elif op.name == 'open':
                result = self.manager.take(open(*op.args, **op.kwargs))
                op.fd = result
            elif op.name == 'interaction':
                result = op.callable(op.fd.fd, *op.args, **op.kwargs)
            else:
                result = getattr(op.fd.fd, op.name)(*op.args, **op.kwargs)
            op.state = 'success'
            threads.blockingCallFromThread(self.manager.reactor, op.deferred.callback, result)
        except Exception as e:
            op.state = 'failure'
            threads.blockingCallFromThread(self.manager.reactor, op.deferred.errback, e)
        finally:
            self.busy = False

        
    def __call__(self):
        self.running = True
        while self.running:
            try:
                op = self.queue.get(True, self.manager.queue_timeout)
                self.execute(op)
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
        self.maxrunners = maxrunners
        self.queue_timeout = queue_timeout
        self.runners = []
        for i in range(self.maxrunners - 1):
            self.spawnRunner()
        self.runner_no = 0
        self.stopdeferred = defer.Deferred()
        self.reactor.addSystemEventTrigger('before','shutdown',self.stop)

    def take(self, fileobject):
        """
        @param fileobject: a file-like object
        @return: FileIOProxy
        
        manager.take(myfile) -> FileIOProxy wrapped around myfile
        """
        fp = FileIOProxy(self, fileobject)
        return fp

    def enqueue(self, op, fd, args, kwargs, proc=None):
        """
        @param op: operation name
        @param fd: FileIOProxy instance or None for 'open' and 'interaction'
        @param args: additional arguments for the operation
        @param kwargs: additional keyword arguments for the operation
        @param proc: a callable to be used with op='interaction'
        
        @return: a Deferred which fires with operation's result
        """
        if not self.accept_operations:
            raise RuntimeError("Operations queued past service stop")
        d = defer.Deferred()
        operation = Operation(name=op, args=args, kwargs=kwargs, deferred=d, fd=fd, callable=proc)
        
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
            x.enqueue(Operation(op='_stop'))
        
    def open(self, *args, **kwargs):
        return self.enqueue('open', None, args, kwargs)
