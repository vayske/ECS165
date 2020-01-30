from template.config import *


class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)
    def has_capacity(self):
        if self.num_records < PAGE_SIZE / MAX_ENTRY_SIZE:
            return True
        return False

    def write(self, value):
        #Write in Physical Data
        self.num_records += 1


