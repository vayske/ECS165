from lstore.config import *

class MyLock:

    def __init__(self):
        self.lock_manage = {}

    def lock_read(self, rid):
        if rid in self.lock_manage:
            readlock, writelock = self.lock_manage[rid]
            if writelock > 0:
                return False
            else:
                readlock += 1
                self.lock_manage[rid] = (readlock, writelock)
                return True
        else:
            self.lock_manage[rid] = (1, 0)
            return True

    def lock_write(self, rid):
        if rid in self.lock_manage:
            readlock, writelock = self.lock_manage[rid]
            if readlock > 0 or writelock > 0:
                return False
            else:
                self.lock_manage[rid] = (0, 1)
                return True
        else:
            self.lock_manage[rid] = (0, 1)
            return True
    def unlock_read(self, rid):
        readlock, writelock = self.lock_manage[rid]
        readlock -= 1
        self.lock_manage[rid] = (readlock, writelock)
        return True

    def unlock_write(self, rid):
        readlock, writelock = self.lock_manage[rid]
        writelock -= 1
        self.lock_manage[rid] = (readlock, writelock)
        return True

    def upgrade(self, rid):
        readlock, writelock = self.lock_manage[rid]
        readlock -= 1
        writelock += 1
        self.lock_manage[rid] = (readlock, writelock)