import logging
from toysql.statement import SelectStatement, InsertStatement


class VM:
    def __init__(self):
        self.table = {}

    def execute(self, statement: SelectStatement | InsertStatement):
        if isinstance(statement, SelectStatement):
            logging.info("this is where we do a select")
            results = []
            print(self.table)
            for id, row in self.table.items():
                results.append(row)

            return results

        if isinstance(statement, InsertStatement):
            logging.info("this is where we do a select")
            if statement.row:
                id, name, email = statement.row
                self.table[id] = (id, name, email)
