## this is our main program
import json
from collections import defaultdict
import os
import csv

class SQL_Database:
    def __init__(self):
        self.tables = {"Customer":["customer_id", "name", "email", "age", "gender"],
                       "Product":["product_id", "p_name"],
                       "Ticket":["ticket_id", "customer_id", "product_id", "date_of_purchase", "ticket_type", "ticket_subject", "ticket_description", "ticket_status", "resolution", "priority", "channel", "first_response_time", "time_to_resolution", "rating"]}

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

    def get(self, table, columns, connect_table=None, on_condition=None, conditions=None, grouping=None, ordering=None, order_by=None):
        print(f"output columns {columns} from table {table} connect with table {connect_table} on {on_condition} with conditions {conditions} gather by {grouping} order by {order_by} in {ordering} order")
        ##### Projection
        # check table name and columns
        
        if table not in self.tables:
            print(f"Table {table} doesn't exist.")
            return
                
        for c in columns:
            if c not in self.tables[table]:
                print(f"Column {c} doesn't exist or unable to get.")
                return
        # Open the JSON file
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
    def __init__(self, schema):
        self.schema = {
        'provider_variables': {
          'brand_name_rx_count': int,
          'gender': str,
          'generic_rx_count': int,
          'region': str,
          'settlement_type': str,
          'specialty': str,
          'years_practicing': int
          },
        'npi': str,
        'cms_prescription_counts': dict
        }
        self.records = []

    def insert(self, data):
        if self.validate(data):
            self.records.append(data)
        else:
            raise ValueError("Data does not conform to schema")
        # table_name = table_info[0]
        # table_columns = " ".join(table_info[1:]).replace("(","").replace(")","").replace(",","")
        # table_columns = table_columns.split(" ")
        # print(f"Put values {values} to table {table_name} on columns {table_columns}")
        # if table_name not in self.tables:
        #     print(f"Table {table_name} doesn't exist.")
        # if len(values) != len(self.tables[table_name]):
        #     print("Number of columns doesn't match.")
        # for i in range(self.tables[table_name]):
        #     self.tables[table_name][i].append(values[i])

    def validate(self, data):
        # Validate that each key in the schema is present in the data and matches the type
        for key, value_type in self.schema.items():
            if key not in data:
                print(f"Missing key in data: {key}")
                return False
            if not isinstance(data[key], value_type):
                print(f"Incorrect type for key: {key}. Expected {value_type}, got {type(data[key])}.")
                return False
            # If the key is 'cms_prescription_counts', it's expected to be a dictionary with string keys and int values
            if key == 'cms_prescription_counts':
                if not all(isinstance(k, str) and isinstance(v, int) for k, v in data[key].items()):
                    print("Invalid 'cms_prescription_counts' format.")
                    return False

    def delete(self, conditions):
        self.records = [record for record in self.records if
                        not all(record.get(field) == value for field, value in conditions.items())]
        # if table_name not in self.tables:
        #     print(f"Table {table_name} doesn't exist.")
        # if not condition:
        #     for ele in items:
        #         self.tables[table_name][ele] = None

    def update(self, new_data, unique_id_field, update_conditions=None):
        updated = False
        if unique_id_field = 'npi':
            file_paths = ['nosql_tables/npi/first_2000_npi.json', 'nosql_tables/npi/2000_to_4000_npi.json', 'nosql_tables/npi/4000_to_6000_npi.json']
        if unique_id_field = 'cms_prescription_counts':
            file_paths = ['nosql_tables/cms_prescription_counts/first_2000_cms_prescription_counts.json', 'nosql_tables/cms_prescription_counts/2000_to_4000_cms_prescription_counts.json', 'nosql_tables/cms_prescription_counts/4000_to_6000_cms_prescription_counts.json']
        if unique_id_field = 'provider_variables':
            file_paths = ['nosql_tables/provider_variables/first_2000_provider_variables.json', 'nosql_tables/provider_variables/2000_to_4000_provider_variables.json', 'nosql_tables/provider_variables/4000_to_6000_provider_variables.json']
        for file_path in file_paths:
            # Proceed only if the file exists to avoid creating a new one unnecessarily
            if not os.path.isfile(file_path):
                continue

            temp_file_path = file_path + '.tmp'  # Temporary file for updates
            with open(file_path, 'r') as file, open(temp_file_path, 'w') as temp_file:
                for line in file:
                    record = json.loads(line)
                    # Check if the record matches the unique identifier and update conditions
                    if (record.get(unique_id_field) == new_data.get(unique_id_field) and
                            (update_conditions is None or all(record.get(cond) == val for cond, val in update_conditions.items()))):
                        record.update(new_data)  # Update the record with new data
                        updated = True
                    # Write the original or updated record to the temporary file
                    temp_file.write(json.dumps(record) + '\n')

            # Replace the original file with the updated temporary file
            os.replace(temp_file_path, file_path)
        
            # If we updated a record, no need to check the remaining files
            if updated:
                break

        if not updated:
            # If the record was not found and updated, you can decide to append it to one of the files or raise an error
            # For example, appending to the first file:
            with open(file_paths[0] + '.tmp', 'a') as temp_file:  # Open in append mode
                temp_file.write(json.dumps(new_data) + '\n')
            os.replace(file_paths[0] + '.tmp', file_paths[0])

    def get(self, conditions=None, order_by=None, limit=None):
        # print(f"output columns {columns} from table {table_name} connect with table {connect_table} on {on_condition} with conditions {conditions} gather by {grouping} order by {order_by} in {ordering} order")
        def match_conditions(record, conditions):
            return all(record.get(field) == value for field, value in conditions.items())

        # Apply conditions if any
        if conditions:
            filtered_records = filter(lambda record: match_conditions(record, conditions), self.records)
        else:
            filtered_records = self.records

        # Sort records if order_by is specified
        if order_by:
            reverse_order = False
            if isinstance(order_by, str) and order_by.startswith('-'):
                order_by = order_by[1:]  # remove '-' sign for descending order indication
                reverse_order = True
            filtered_records = sorted(filtered_records, key=lambda x: x.get(order_by), reverse=reverse_order)

        # Apply limit if specified
        if limit is not None:
            filtered_records = filtered_records[:limit]

        return list(filtered_records)
