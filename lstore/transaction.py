from lstore.table import Table, Record
from lstore.index import Index

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
        self.results = []   #record success or failure to decide commit/abort
        self.locks_rid = [] #record all locks to release later 
        pass

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, *args):
        # to run the query:
        # query.method(*args)
        self.queries.append((query, args))

    def run(self):
        for query, args in self.queries:
            result = query(*args, transaction = self)
            if result[-1] == False:
                #if failure abort right away
                return self.abort()
            self.results.append(result)     #save success return value, might need to used for undo 
        return self.commit()

    def abort(self):
        #undo all previous success
        for (i,result) in enumerate(self.results):
            query = self.queries[i]
            if query.__name__ == 'insert':
                query(result, undo = True)      #pass in result as columns so that query knows what to undo
            elif query.__name__ == 'update':
                query(0, result, undo = True)   # 0 is the key, not used in undo just filling the argument
            elif query.__name__ == 'delete':
                query(result[0], undo = True)   #result[0] is the rid deleted, passed in as key, because delete doesn't have columns argument
            elif query.__name__ == 'increment':
                query(result[0], result[1], undo = True)    #result[0] is the rid, passed in as key
                                                            #result[1] is the old indirection, passed in as column
            # Select and Sum don't need undo because they don't change anything 
        self.commit() #release locks
        return False

    def commit(self):
        #release locks 
        table = self.queries[0].table
        for rid in self.locks_rid:
            table.lock_manager.release_lock(rid, self)
        return True

