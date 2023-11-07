## this is our main program
import json
import csv
import re
class SQL_Database:
    def __init__(self):
        self.tables = {"Customer":["customer_id", "name", "email", "age", "gender"],
                       "Product":["product_id", "p_name"],
                       "Ticket":["ticket_id", "customer_id", "product_id", "date_of_purchase", "ticket_type", "ticket_subject", "ticket_description", "ticket_status", "resolution", "priority", "channel", "first_response_time", "time_to_resolution", "rating"]}
        self.condition_operators = ["=", ">", "<", ">=", "<=", "!=", "LIKE", "IN", "NOTIN"] #NOTIN is NOT IN
        self.condition_logics = ["AND", "OR"]
    def insert(self, table_info, values):
        
        table_name = table_info[0]
        table_columns = " ".join(table_info[1:]).replace("(","").replace(")","").replace(",","")
        table_columns = table_columns.split(" ")
        print(f"Put values {values} to table {table_name} on columns {table_columns}")
        
        if table_name not in self.tables:
            print(f"Table {table_name} doesn't exist.")
            return
        if len(values) != len(self.tables[table_name]):
            print("Number of columns doesn't match.")
            return
        # Open the JSON file
        with open(f'sql_tables/{table_name}/metadata.json', 'r') as file:
            # Load JSON data from the file into a Python object
            metadata = json.load(file)
            last_chunk = max(metadata)
            if metadata[last_chunk]>=2000: # if the last chunk is full then create new chunk
                last_chunk += 1
                metadata[last_chunk] = 0

    def delete(self, table_name, items, condition):
        print(f"delete items {items} from table {table_name} on condition {condition}")
        if table_name not in self.tables:
            print(f"Table {table_name} doesn't exist.")
        if not condition:
            for ele in items:
                self.tables[table_name][ele] = None

    def update(self, table_name, values, condition):
        print(f"update values {values} from table {table_name} on condition {condition}")
        if table_name not in self.tables:
            print(f"Table {table_name} doesn't exist.")
    def check_condition(self, targets, operators, comparisons, logics):

        def sql_like_to_regex(pattern):
            # Escape special characters in regex
            pattern = re.escape(pattern)
            # Replace SQL LIKE escaped sequences with regex escaped sequences
            pattern = pattern.replace(r'\%', '%').replace(r'\_', '_')
            # Convert SQL LIKE wildcards to regex wildcards
            pattern = pattern.replace('%', '.*').replace('_', '.')
            # Match the entire string
            pattern = '^' + pattern + '$'
            # Compile the regex pattern
            return re.compile(pattern)

        def like(string, sql_like_pattern):
            regex = sql_like_to_regex(sql_like_pattern)
            return regex.match(string) is not None
        
        pass_condition = True
        i = 0
        while i < len(logics)+1:
            op = operators[i]
            if op == "=":
                pass_condition = targets[i]==comparisons[i]
            elif op == ">":
                pass_condition = targets[i]>comparisons[i]
            elif op == "<":
                pass_condition = targets[i]<comparisons[i]
            elif op == ">=":
                pass_condition = targets[i]>=comparisons[i]
            elif op == "<=":
                pass_condition = targets[i]<=comparisons[i]
            elif op == "!=":
                pass_condition = targets[i]!=comparisons[i]
            elif op == "LIKE":
                pass_condition = like(targets[i], comparisons[i].strip("'").strip('"'))

            elif op == "IN":
                print(comparisons[i].strip('[]').replace('"','').replace("'",'').split(','))
                pass_condition = targets[i] in comparisons[i].strip('[]').replace('"','').replace("'",'').split(',')
            else: #NOTIN
                pass_condition = targets[i]  not in comparisons[i]
            
            while i < len(logics):
                if logics[i] == "OR" and pass_condition:
                    return True
                elif logics[i] == "AND" and not pass_condition:
                    i+=1
                else:
                    break
            i+=1
        return pass_condition
    
    def parse_condition(self, conditions):
        condition_targets = []
        condition_operators = []
        condition_comparisons = []
        condition_logics = []
        valid_condition = True
        if conditions:

            if len(conditions) < 3:
                valid_condition = False
            elif len(conditions) == 3:
                condition_targets.append(conditions[0])
                if conditions[1] not in self.condition_operators:
                    valid_condition = False
                condition_operators.append(conditions[1])
                condition_comparisons.append(conditions[2])
            elif len(conditions) % 4 == 3:
                count = 0
                for i in range(len(conditions) // 4):
                    condition_targets.append(conditions[count])
                    if conditions[count+1] not in self.condition_operators:
                        valid_condition = False
                    condition_operators.append(conditions[count+1])
                    condition_comparisons.append(conditions[count+2])
                    if conditions[count+3] not in self.condition_logics:
                        valid_condition = False
                        break
                    condition_logics.append(conditions[count+3])
                    count+=4
                condition_targets.append(conditions[count])
                if conditions[count+1] not in self.condition_operators:
                    valid_condition = False
                condition_operators.append(conditions[count+1])
                condition_comparisons.append(conditions[count+2])
            else:
                valid_condition = False

        return condition_targets, condition_operators, condition_comparisons, condition_logics, valid_condition
    
    def get(self, table, columns, connect_table=None, on_condition=None, conditions=None, grouping=None, ordering=None, order_by=None):
        print(f"output columns {columns} from table {table} connect with table {connect_table} on {on_condition} with conditions {conditions} gather by {grouping} order by {order_by} in {ordering} order")
        #### parse condition (WHEN)
        condition_targets, condition_operators, condition_comparisons, condition_logics, valid_condition = self.parse_condition(conditions)
        if not valid_condition:
            print("Error: Invalid condition.")
            return
        print(self.check_condition(condition_targets,condition_operators,condition_comparisons,condition_logics))
        return
        ##### Projection
        # check table name and columns
        if table not in self.tables:
            print(f"Table {table} doesn't exist.")
            return
                
        for c in columns:
            if c not in self.tables[table]:
                print(f"Column {c} doesn't exist or unable to get.")
                return
        # Open the metadata JSON file
        with open(f'sql_tables/{table}/metadata.json', 'r') as file:
            # Load JSON data from the file into a Python object
            metadata = json.load(file)
        column_print=f'|{" | ".join(columns)}|'
        print(column_print)
        print('-'*len(column_print))
        target_index = [self.tables[table].index(c) for c in columns]
        for table_chunk_num in metadata:
            with open(f'sql_tables/{table}/table_{table_chunk_num}.csv', 'r') as csvfile:
                # Create a CSV reader object
                csvreader = csv.reader(csvfile)
                next(csvreader)
                for row in csvreader:
                    values_to_print = []
                    for ti in target_index:
                        values_to_print.append(row[ti])
                    print(values_to_print)
class noSQL_Database:
    def __init__(self):
        self.tables = {}

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

    def update(self, table_name, values, condition):
        print(f"update values {values} from table {table_name} on condition {condition}")
        if table_name not in self.tables:
            print(f"Table {table_name} doesn't exist.")

    def get(self, table_name, columns, connect_table, on_condition, conditions=None, grouping=None, ordering=None, order_by=None):
        print(f"output columns {columns} from table {table_name} connect with table {connect_table} on {on_condition} with conditions {conditions} gather by {grouping} order by {order_by} in {ordering} order")