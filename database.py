## this is our main program
class Database:
    def __init__(self):
        self.tables = {}

    # def create_table(self, table_name, columns):
    #     if table_name in self.tables:
    #         raise ValueError(f"Table {table_name} already exists.")
    #     self.tables[table_name] = {"columns": columns, "data": []}

    def insert(self, table_info, values):
        
        table_name = table_info[0]
        table_columns = " ".join(table_info[1:]).replace("(","").replace(")","").replace(",","")
        table_columns = table_columns.split(" ")
        print(f"Put values {values} to table {table_name} on columns {table_columns}")
        if table_name not in self.tables:
            print(f"Table {table_name} doesn't exist.")
        if len(values) != len(self.tables[table_name]):
            print("Number of columns doesn't match.")
        for i in range(self.tables[table_name]):
            self.tables[table_name][i].append(values[i])

    def delete(self, table_name, items, condition):
        print(f"delete items {items} from table {table_name} on condition {condition}")
        if table_name not in self.tables:
            print(f"Table {table_name} doesn't exist.")
        if not condition:
            for ele in items:
                self.tables[table_name][ele] = None
        #else:
            #evaluate_conditions()

    def update(self, table_name, values, condition):
        print(f"update values {values} from table {table_name} on condition {condition}")
        if table_name not in self.tables:
            print(f"Table {table_name} doesn't exist.")
            #evaluate_conditions()

    def get(self, table_name, columns, connect_table, on_condition, conditions=None, grouping=None, ordering=None, order_by=None):
        print(f"output columns {columns} from table {table_name} connect with table {connect_table} on {on_condition} with conditions {conditions} gather by {grouping} order by {order_by} in {ordering} order")
        # if table_name not in self.tables:
        #     raise ValueError(f"Table {table_name} doesn't exist.")
        # data = self.tables[table_name]["data"]

        # if not conditions:
        #     results = data
        # else:
        #     results = []
        #     for row in data:
        #         if self._evaluate_conditions(row, conditions):
        #             results.append(row)

        # if not columns or columns == ["*"]:
        #     return results
        # return [self._filter_columns(row, columns) for row in results]

    # def _evaluate_conditions(self, row, conditions):
    #     # Implement your condition evaluation logic here
    #     # For simplicity, this template assumes that all conditions are met
    #     return True

    # def _filter_columns(self, row, columns):
    #     return {col: row[col] for col in columns}
