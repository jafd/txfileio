"""
Interfaces used by txfileio implementations.
"""
from zope.interface import Interface, Attribute

class IRunner(Interface):
    running = Attribute("(read-only) If True, the worker is currently running")
    busy = Attribute("(read-only) If True, the worker is currently busy with a file")
    fd = Attribute("(read-only) The current file object (IProxy) the runner is working on or the last one it has been working on")
    def enqueue(self, operation):
        """
        @param operation: an IOperation implementor
        @return: Deferred -> operation's result
        
        Enqueue an operation.
        """
    
    def __call__(self):
        """
        The main worker loop.
        """
    
    def stop(self):
        """
        Stop the worker.
        """
    
    def execute(self, operation):
        """
        @param operation: an IOperation implementor
        @return: the operation's result
        
        Execute a single operation. For multithreaded implementations,
        it is called in a worker thread.
        """

class IOperation(Interface):
    fd = Attribute("The IProxy implementor object the operation should be carried out on")
    name = Attribute("The name of operation. Some operations are special to IManager instance, and cannot be used on the file directly.")
    args = Attribute("Additional arguments passed to the operation")
    kwargs = Attribute("Additional keyword arguments passed to the operation")
    callable = Attribute("A callable to run if name == 'interaction'")
    state = Attribute("The state of operation: new, running, succeeded, failed, cancelled")

class IManager(Interface):
    def open(self, *args, **kwargs):
        """
        @param *args: arguments as for open() builtin
        @param **kwargs: keyword arguments as for open() builtin
        
        @return: Deferred -> IProxy implementor
        
        Open a file object and return a Deferred which will fire with its proxy.
        """
    
    def take(self, filelike):
        """
        @param filelike: a file-like object
        @return: an IProxy wrapped around the object
        """
    
    def enqueue(self, op, fd, args, kwargs, proc=None):
        """
        @param op: the operation name
        @param fd: an IProxy to work on
        @param args: arguments to pass to the operation
        @param kwargs: keyword arguments to pass to the operation
        @param proc: a callable if op='interaction'
        """
        
    def start(self):
        """
        Launch all workers and accept operations.
        """
    
    def stop(self):
        """
        Stop all workers and shut down.
        """
    
class IProxy(Interface):
    fd = Attribute("The real filelike object. May be None or raise an exception if used with a remote implementation.")
    def __init__(self, manager, filelike):
        """
        The constructor must accept an IManager instance and a file-like object
        """
    
    def runInteraction(self, proc, *args, **kwargs):
        """
        @param proc: a callable
        @param *args: additional arguments passed to the callable
        @param **kwargs: additional keyword arguments passed to the callable
        @return: Deferred -> result of the interaction
        
        Delegate to a worker a callable which uses the filehandle in a blocking manner.
        """
