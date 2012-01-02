from txfileio.threaded import FileIOManager
import sys
from random import random
from twisted.internet import reactor, defer
from copy import copy

openfiles = 200

def filereadtest():
    thefd = None
    def fileopened(fd):
        global openfiles
        print "File %s opened" % fd.name
        return fd

    def wtofile(fd):
        #def _do_write(fd, line):
        #    return fd.write("Test line %s\n" % str(line)).addCallback(lambda _: fd)
        def _finish_writing(_, fd):
            print "File %s written to" % fd.name
            return fd

        def _do_write(fd):
            for i in range(100):
                fd.write("Test line %s\n" % str(i))

        #return fd.runInteraction(_do_write).addCallback(_finish_writing, fd)
        d = fd.write("Test line %s\n" % -1)
        for i in range(100):
            line = "Test line %s\n" % str(i)
            d.addCallback(lambda x, l: fd.write(l), line)
        d.addCallback(lambda x: sys.stdout.write("File %s written to\n" % fd.name))
        return d.addCallback(lambda x:fd)

    def _checkreactor(_):
        global openfiles
        openfiles -= 1
        if openfiles == 0:
            filemanager.stop()
            reactor.stop()

    def fclose(fd):
        print "File %s closed" % fd.name
        return fd.close().addCallback(_checkreactor)

    def ferr(e):
        raise e
        
    filemanager = FileIOManager(reactor, maxrunners=5)
    filemanager.start()
    print "*Opening files started"
    for i in range(openfiles):
        name = "/tmp/test%d" % i
        filemanager.open(name, 'wb').addCallback(fileopened).addCallback(wtofile).addCallback(fclose)
    print "* Opening files ended"

reactor.callLater(0, filereadtest)
reactor.run()

def synctest():
    fds = []
    for i in range(200):
        fds.append(open("/tmp/test{0}".format(i), 'wb'))
        print "File %s opened" % "/tmp/test{0}".format(i)
    for f in fds:
        for j in range (100):
            f.write('Test line %d\n' % j)
        print "File %s written to" % f.name
    for f in fds:
        f.close()
        print "File %s closed" % f.name
