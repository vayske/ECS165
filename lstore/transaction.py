from lstore.table import Table, Record
from lstore.index import Index


class Transaction:
    """
    # Creates a transaction object.
    """

    def __init__(self):
        self.queries = []
        pass

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, 0, *[None, 1, None, 2, None])
    """

    def add_query(self, query, *args):
        self.queries.append((query, args))

    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        i = 0
        for query, args in self.queries:
            result = query(*args)
            # If the query has failed the transaction should abort
            if result == False:
                return self.abort(i)
            i += 1
        return self.commit()

    def abort(self, i):
        # TODO: do roll-back and any other necessary operations
        while i >= 0:
            query, args = self.queries[i]
            query(*args, undo=True)
        return False

    def commit(self):
        # TODO: commit to database
        return True
