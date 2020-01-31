from template.config import *


class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)

    def has_capacity(self):
        if(self.num_records == 512):
            return False
        return True
        pass

    def write(self, value):
        if(isinstance(value, str)):
            value_to_bytes = bytearray(value, 'utf-8')
        else
            value_to_bytes = value.to_bytes(8,'big')
        next_index = self.num_records * 8
        self.data[next_index:next_index+8] = value_to_bytes[0:8]
        self.num_records += 1
        pass

    def change_value(self, slot, value):
        if(isinstance(value, str)):
            value_to_bytes = bytearray(value, 'utf-8')
        else
            value_to_bytes = value.to_bytes(8,'big')
        index = slot * 8
        self.data[index:index+8] = value_to_bytes[0:8]
        pass
