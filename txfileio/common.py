"""
Implementation-independent bits of txfileio.
"""
from zope.interface import implements
from txfileio.interfaces import IProxy, IOperation

class FileIOProxy(object):
    """
    A FileIOProxy proxies calls to file operations, so they all return Deferreds; the operations
    themselves are queued up and delegated to worker threads. Deferred fires when the worker has
    finished the operation.
    """
    implements(IProxy)
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
    implements(IOperation)
    __slots__ = ('fd', 'name', 'deferred', 'args', 'kwargs', 'state', 'callable')
    def __init__(self, **kwargs):
        self.state = 'new'
        for i in ('fd', 'name', 'deferred', 'args', 'kwargs', 'callable'):
            setattr(self, i, kwargs.get(i))
            
    def __str__(self):
        return "File operation: {0}({1}, {2}), state = {3}".format(self.name, repr(self.args), repr(self.kwargs), self.state)
