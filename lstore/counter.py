import threading 
#Counter with lock, used in pin and lock dictionary
class Couter():
    def __init__(self, value=0):
        self.value = value
        self.lock = threading.Lock()

    def inc(self):
        with self.lock:
            self.value += 1
            return self.value

    def dec(self):
        with self.lock:
            if self.value >0:
                self.value -= 1
            return self.value
            
    def get(self):
      return self.value
    
    
