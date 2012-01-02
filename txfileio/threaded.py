# -*- coding: utf-8 -*-
"""
@author: Yaroslav Fedevych <jaroslaw.fedewicz@gmail.com>
@version: 0.0.0

This is a threaded implementation of asynchronous file I/O for Twisted. It aims at increasing
responsiveness in services that do lots of file I/O, sometimes on networked filesystems.

Please note that this implementation is quite dumb: all it tries to do is delegate file I/O operations
to worker threads, and defer the results. It would most definitely slow down execution time, especially
when using lots and lots of small operations. But the aim is not to make things fast, but
to increase responsiveness for the cases when file I/O might be slow and, for example, all clients would
be impacted by a single client causing lags by making the server do slow file I/O.

@todo: abstract enough code to allow different implementation (subprocesses?)

"""
from zope.interface import implements
from twisted.internet import defer, threads
from twisted.python.failure import Failure
import Queue
import sys

from txfileio.common import Operation, FileIOProxy
from txfileio.interfaces import IRunner, IManager

class Runner(object):
    """
    The Runner is a callable object which actually executes I/O operations
    and interactions. It is never used directly.
    """
    implements(IRunner)
    __slots__ = ('fd', 'busy', 'running', 'queue', 'manager')
    def __init__(self, manager):
        self.queue = Queue.Queue()
        self.fd = None
        self.busy = False
        self.running = False
        self.manager = manager

    def enqueue(self, op):
        """
        Enqueue an operation.
        """
        self.queue.put(op)
        return op.deferred
        
    def stop(self):
        """
        Stop the worker gracefully.
        """
        return self.enqueue(Operation(op='_stop'))
        
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
        """
        The main working loop.
        """
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
    implements(IManager)
    def __init__(self, reactor, maxrunners=5, queue_timeout=1):
        self.reactor = reactor
        self.accept_operations = True
        self.maxrunners = maxrunners
        self.queue_timeout = queue_timeout
        self.runners = []
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
    
    def start(self):
        """
        Spawns runners and starts accepting operations.
        """
        for i in range(self.maxrunners - 1):
            self.spawnRunner()
        self.accept_operations = True
    
    def spawnRunner(self):
        """
        Starts a runner in a separate thread.
        """
        r = Runner(self)
        self.runners.append(r)
        return threads.deferToThread(r).addCallback(lambda _: self.runners.remove(r))

    def stop(self):
        """
        Stop the workers. This is added as a reactor hook, and normally does not need to
        be called directly.
        """
        self.accept_operations = False
        for x in self.runners:
            x.stop()
        
    def open(self, *args, **kwargs):
        """
        @param *args: arguments as normally passed to file.open()
        @param **Kwargs: keyword arguments as normally passed to file.open()
        
        @return: a Deferred which fires with a FileIOProxy object.
        
        Opens a file asynchronously. 
        """
        return self.enqueue('open', None, args, kwargs)
