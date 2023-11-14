## this is our main program
import json
from collections import defaultdict
import os
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
        if not targets: ## no conditions
            return True
        
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
        condition_target_index = [self.tables[table].index(ct) for ct in condition_targets]
        for table_chunk_num in metadata:
            with open(f'sql_tables/{table}/table_{table_chunk_num}.csv', 'r') as csvfile:
                # Create a CSV reader object
                csvreader = csv.reader(csvfile)
                next(csvreader)
                rows = list(csvreader)
                for row in rows:
                    input_targets = []
                    if not self.check_condition([row[cti] for cti in condition_target_index], condition_operators, condition_comparisons, condition_logics):
                        continue

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

    def insert(self, table_info=None, json_string=None):
        print(f"Put values {json_string} to table {table_info}")
        # The path to the JSON file
        json_file_path = 'nosql_tables/filtered_data.jsonl'
        new_record = json.loads(json_string)

        # Load the current data from the file
        try:
            with open(json_file_path, 'r') as file:
                data = json.load(file)
        except FileNotFoundError:
            data = []  # If the file does not exist, start with an empty list
        except json.JSONDecodeError:
            raise Exception(f"Error reading the file {json_file_path}. Make sure it's valid JSON.")

        # Append the new record to the data list
        data.append(new_record)

        # Write the updated data back to the file
        with open(json_file_path, 'w') as file:
            json.dump(data, file, indent=4)  # 'indent' for pretty-printing

        print(f"New record added to {json_file_path}")

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

    def delete(self, table_name, items, condition):
        print(f"delete items {items} from table {table_name} on condition {condition}")
        fields_to_delete = items[0]
        concatenated = ''.join(condition)
        # Step 2: Replace single quotes with double quotes
        valid_json_string = concatenated.replace("'", '"')
        search_criteria = json.loads(valid_json_string)
        file_paths = ['filtered_data.jsonl', 'first_2000_records.json',
                      'records_2000_to_4000.json',
                      'records_4000_to_6000.json']
        def get_nested_value(dic, keys):
            """Recursively fetches nested values from a dictionary using a list of keys."""
            for key in keys:
                if isinstance(dic, dict):
                    dic = dic.get(key, None)
                else:  # If the path is broken, return None
                    return None
            return dic

        # Read the data from the file
        for file_path in file_paths:
            with open(file_path, 'r') as file:
                data = json.load(file)
            for record in data:
                if all(get_nested_value(record, key.split('.')) == value for key, value in search_criteria.items()):
                    if fields_to_delete != None:
                        field_parts = fields_to_delete.split('.')

                        # field_parts now contains ['provider_variables', 'region']
                        parent_key = field_parts[0]  # 'provider_variables'
                        child_key = field_parts[1] if len(field_parts) > 1 else None  # 'region'
                        record.get(parent_key, {}).pop(child_key, None)
                        with open(file_path, 'w') as file:
                            json.dump(data, file, indent=4)

                        print(f"Region field removed from the record")


    def update(self, table_name, Values, condition):
        print(f"update values {Values} from table {table_name} on condition {condition}")
        values = Values[0]
        fields_to_change = str(table_name)
        concatenated = ''.join(condition)
        # Step 2: Replace single quotes with double quotes
        valid_json_string = concatenated.replace("'", '"')
        search_criteria = json.loads(valid_json_string)
        file_paths = ['filtered_data.jsonl', 'first_2000_records.json',
                      'records_2000_to_4000.json',
                      'records_4000_to_6000.json']

        def get_nested_value(dic, keys):
            """Recursively fetches nested values from a dictionary using a list of keys."""
            for key in keys:
                if isinstance(dic, dict):
                    dic = dic.get(key, None)
                else:  # If the path is broken, return None
                    return None
            return dic

        # Read the data from the file
        for file_path in file_paths:
            with open(file_path, 'r') as file:
                data = json.load(file)
            for record in data:
                if all(get_nested_value(record, key.split('.')) == value for key, value in search_criteria.items()):
                    if fields_to_change != None:
                        field_parts = fields_to_change.split('.')

                        # field_parts now contains ['provider_variables', 'region']
                        parent_key = field_parts[0]  # 'provider_variables'
                        child_key = field_parts[1] if len(field_parts) > 1 else None  # 'region'
                        record[parent_key][child_key] = values
                        # Step 3: Save the Data Back

                        with open(file_path, 'w') as file:
                            json.dump(data, file, indent=4)

    def get(self, table=None, fields_to_return=None, connect_table=None, on_condition=None, conditions=None, grouping=None, ordering=None, order_by=None):
        print(f"output columns {fields_to_return} from table {table} connect with table {connect_table} on {on_condition} with conditions {conditions} gather by {grouping} order by {order_by} in {ordering} order")

        if conditions != None:
            str_dict = ' '.join(conditions)
            str_dict = str_dict.replace("'{", '{"').replace("}'", '"}').replace(': "', ':"').replace('"}', '"}')
            search_criteria = eval(str_dict)
        else:
            search_criteria = None
        if grouping != None:
            grouping = ["'provider_variables.region'"]
            string_from_list = grouping[0]
            group_by_field = string_from_list.strip("'")
        if ordering != None or order_by != None:
            string_from_list_1 = ordering[0]
            order_by_field = string_from_list_1.strip("'")
        else:
            order_by_field = None
        def get_nested_value(dic, keys):
            """Recursively fetches nested values from a dictionary using a list of keys."""
            for key in keys:
                if isinstance(dic, dict):
                    dic = dic.get(key, None)
                else:  # If the path is broken, return None
                    return None
            return dic
        file_paths = ['nosql_tables/filtered_data.jsonl', 'nosql_tables/first_2000_records.json',
                      'nosql_tables/records_2000_to_4000.json',
                      'nosql_tables/records_4000_to_6000.json']
        grouped_records = defaultdict(list)
        for file_path in file_paths:
            with open(file_path, 'r') as file:
                data = json.load(file)
            # And we have search criteria

            # We want to find all records that match our search criteria
            for record in data:
                if all(get_nested_value(record, key.split('.')) == value for key, value in search_criteria.items()):
                    if fields_to_return != None:
                        extracted_record = {field: get_nested_value(record, field.split('.')) for field in
                                            fields_to_return}
                        # Append the extracted record to the list in the grouped_records dictionary
                        group_value = get_nested_value(record, list(search_criteria.keys())[0].split('.'))
                        grouped_records[group_value].append(extracted_record)
                    else:
                        key_to_split = next(iter(search_criteria))
                        group_value = get_nested_value(data, key_to_split.split('.'))
                        grouped_records[group_value].append(data)

        # print(grouped_records)
        # Output would be [{'name': 'John Doe', 'age': 30, 'city': 'New York'}] assuming these are the only matching records

        # Sort and reduce the records if necessary
        for group, records in grouped_records.items():
            if order_by_field:
                def merge_sort(records, get_key, default_value=float('-inf')):
                    if len(records) <= 1:
                        return records

                    def merge(left, right, get_key, default_value):
                        merged = []
                        left_index = right_index = 0

                        while left_index < len(left) and right_index < len(right):
                            left_record = left[left_index]
                            right_record = right[right_index]

                            # Use the provided get_key function with a default value if the key is not found or if the value is None
                            left_key_value = get_key(left_record) if get_key(left_record) is not None else default_value
                            right_key_value = get_key(right_record) if get_key(
                                right_record) is not None else default_value

                            if left_key_value <= right_key_value:
                                merged.append(left_record)
                                left_index += 1
                            else:
                                merged.append(right_record)
                                right_index += 1

                        # Add the remaining parts
                        merged.extend(left[left_index:])
                        merged.extend(right[right_index:])
                        return merged

                    # Divide the list into two halves
                    middle_index = len(records) // 2
                    left_half = merge_sort(records[:middle_index], get_key, default_value)
                    right_half = merge_sort(records[middle_index:], get_key, default_value)

                    # Merge the sorted halves
                    return merge(left_half, right_half, get_key, default_value)

                # Usage example with the sorting function
                for file_path in file_paths:
                    with open(file_path, 'r') as file:
                        data = json.load(file)
                    # And we have search criteria
                    for record in data:
                        if all(get_nested_value(record, key.split('.')) == value for key, value in
                               search_criteria.items()):
                            # group_value = get_nested_value(record, group_by_field.split('.'))
                            # grouped_records[group_value].append(record)
                            extracted_record = {field: get_nested_value(record, field.split('.')) for field in
                                                fields_to_return}
                            # Append the extracted record to the list in the grouped_records dictionary
                            group_value = get_nested_value(record, list(search_criteria.keys())[0].split('.'))
                            grouped_records[group_value].append(extracted_record)

                # Now using merge_sort to sort each group
                for group, records in grouped_records.items():
                    key_function = lambda x: get_nested_value(x, order_by_field.split('.'))
                    sorted_records = merge_sort(records, key_function)
                    if ordering == "DSC":
                        sorted_records = sorted_records[::-1]  # Reverse for descending order
                    grouped_records[group] = sorted_records

                # Print the sorted grouped records
                # for group, records in grouped_records.items():
                #     print(f"Group: {group}")
                #     for record in records:
                #         print(record)

                # If specific fields to return are specified, pluck those out
            # if return_fields:
            #      grouped_records[group] = [{field: get_nested_value(record, field.split('.')) for field in return_fields} for record in records]
            print(grouped_records)
