## this is our main program
import json
from collections import defaultdict
import os
import csv
import re
import shutil

class SQL_Database:
    def __init__(self):
        self.tables = {"Customer":["customer_id", "name", "email", "age", "gender"],
                       "Product":["product_id", "p_name"],
                       "Ticket":["ticket_id", "customer_id", "product_id", "date_of_purchase", "ticket_type", "ticket_subject", "ticket_description", "ticket_status", "resolution", "priority", "channel", "first_response_time", "time_to_resolution", "rating"]}
        self.condition_operators = ["=", ">", "<", ">=", "<=", "!=", "LIKE", "IN", "NOTIN"] #NOTIN is NOT IN
        self.condition_logics = ["AND", "OR"]
        self.tables_primary_key = {"Customer":"customer_id",
                       "Product":"product_id",
                       "Ticket":"ticket_id"}
        self.tables_foreign_key = {"Customer":{},
                       "Product":{},
                       "Ticket":{1:("Customer", "customer_id"),
                                 2:("Product", "product_id")}}
    def check_key(self, table, value_list, ignore=None):
        # step 1: check primary key
        pk_index = self.tables[table].index(self.tables_primary_key[table])
        inserted_pk = value_list[pk_index]
        if not inserted_pk: # check if the value is ''
            print(f'Error:Primary key could not be null')
            return False
        # Open the JSON file
        with open(f'sql_tables/{table}/metadata.json', 'r') as file:
            # Load JSON data from the file into a Python object
            metadata = json.load(file)
        for table_chunk_num in metadata:
            with open(f'sql_tables/{table}/table_{table_chunk_num}.csv', 'r') as csvfile:
                # Create a CSV reader object
                csvreader = csv.reader(csvfile)
                header = next(csvreader)
                for r in csvreader:
                    if ignore and r == ignore:
                        continue
                    if r[pk_index] == inserted_pk:
                        print(f'Error: Primary key {inserted_pk} exists. Could not PUT this row to table.')
                        return False
        
        # step 2: check foreign key
        
        for i, fk in self.tables_foreign_key[table].items():
            inserted_fk = value_list[i]
            if not inserted_fk: # check if the value is ''
                print(f'Error: Foreign key {self.tables[table][i]} could not be null')
                return False
            fk_table = fk[0]
            fk_column = fk[1]
            fk_index = self.tables[fk_table].index(fk_column)
            fk_exist = False
            # Open the JSON file
            with open(f'sql_tables/{fk_table}/metadata.json', 'r') as file:
                # Load JSON data from the file into a Python object
                metadata = json.load(file)
            for table_chunk_num in metadata:
                with open(f'sql_tables/{fk_table}/table_{table_chunk_num}.csv', 'r') as csvfile:
                    # Create a CSV reader object
                    csvreader = csv.reader(csvfile)
                    header = next(csvreader)
                    for r in csvreader:
                        if r[fk_index] == inserted_fk:
                            fk_exist = True
                            break
                    if fk_exist:
                        break
            if not fk_exist:
                print(f'Error: inserted value {inserted_fk} is foreign key, but it does not exist in foriegn table {fk_table}-{fk_column}')
                return False
        return True
    
    def insert(self, table, values):
        
        if table not in self.tables:
            print(f"Error: {table} is not a valid table to PUT.")
            return
        
        values_list = values.split(',')
        for _ in range(len(self.tables[table]) - len(values_list)):
            values_list.append('')
        for i, v in enumerate(values_list):
            values_list[i] = v.strip(' ')

        if len(values_list) > len(self.tables[table]):
            print("Error: inserted value exeed the column length.")
            return
        print(f"Put values {values_list} to table {table} on columns {self.tables[table]}")

        if not self.check_key(table, values_list.copy()):
            print("Error: Value check does'n pass, cannot insert this value to table")
            return
        # Open the JSON file
        with open(f'sql_tables/{table}/metadata.json', 'r') as file:
            # Load JSON data from the file into a Python object
            metadata = json.load(file)
        for c in metadata:
            last_chunk = c
        
        print(last_chunk)
        if metadata[last_chunk]>=2000: # if the last chunk is full then create new chunk
            last_chunk += 1
            metadata[last_chunk] = 0
            with open(f'sql_tables/{table}/table_{last_chunk}.csv', mode='w', newline='') as file:
                writer = csv.writer(file)
                # Write rows to the CSV file
                writer.writerow(self.tables[table]) #write header first
                writer.writerow(values_list)
        else:
            with open(f'sql_tables/{table}/table_{last_chunk}.csv', mode='a', newline='') as file:
                writer = csv.writer(file)
                # Write rows to the CSV file
                writer.writerow(values_list)
        metadata[last_chunk] += 1
        # Open the JSON file
        with open(f'sql_tables/{table}/metadata.json', 'w') as file:
            # save the new metadata to metadata json
            json.dump(metadata, file, indent=4)
        
    def fill_chunks(self, table):
        chunk_num_to_write_queue = []
        # Open the JSON file
        with open(f'sql_tables/{table}/metadata.json', 'r') as file:
            # Load JSON data from the file into a Python object
            metadata = json.load(file)
        new_metadata = metadata.copy()
        for c in metadata:
            last_chunk_num = c
        buffer = []
        read_count = 0
        for table_chunk_num in metadata:
            if read_count or (metadata[table_chunk_num] < 2000 and table_chunk_num != last_chunk_num):
                chunk_num_to_write_queue.append(table_chunk_num)
                with open(f'sql_tables/{table}/table_{table_chunk_num}.csv', 'r') as csvfile:
                    # Create a CSV reader object
                    csvreader = csv.reader(csvfile)
                    header = next(csvreader)
                    for row in csvreader:
                        buffer.append(row)
                        read_count+=1
                        # if buffer is full, write to new file
                        if read_count == 2000:
                            chunk_num_to_write = chunk_num_to_write_queue.pop(0)
                            print('group')
                            print(chunk_num_to_write)
                            print(chunk_num_to_write_queue)
                            with open(f'sql_tables/{table}/table_{chunk_num_to_write}.csv', mode='w', newline='') as file:
                                writer = csv.writer(file)
                                # Write rows to the CSV file
                                writer.writerow(header) #write header first
                                for r in buffer:
                                    writer.writerow(r)
                            new_metadata[chunk_num_to_write] = read_count
                            buffer = []
                            read_count = 0
                            
        if read_count:
            print(f'left:{read_count}')
            print(chunk_num_to_write_queue)
            chunk_num_to_write = chunk_num_to_write_queue.pop(0)
            with open(f'sql_tables/{table}/table_{chunk_num_to_write}.csv', mode='w', newline='') as file:
                writer = csv.writer(file)
                # Write rows to the CSV file
                writer.writerow(header) #write header first
                for r in buffer:
                    writer.writerow(r)
                new_metadata[chunk_num_to_write] = read_count
                buffer = []
                read_count = 0
        for left_c in chunk_num_to_write_queue:
            if os.path.exists(f'sql_tables/{table}/table_{left_c}.csv'):
                os.remove(f'sql_tables/{table}/table_{left_c}.csv')
                del new_metadata[left_c]
        # Open the JSON file
        with open(f'sql_tables/{table}/metadata.json', 'w') as file:
            # save the new metadata to metadata json
            json.dump(new_metadata, file, indent=4)
        
    def delete(self, table, items, conditions):
        print(f"delete rows from table {table} on condition {conditions}")
        table = table[0]
        if table not in self.tables:
            print(f"Error: Table {table} doesn't exist.")
            return
        if not conditions:
            print(f'Error: Need condition(ex. WHEN name = Jane) to DROP data.')
            return

        #### parse condition (WHEN)
        condition_targets, condition_operators, condition_comparisons, condition_logics, valid_condition = self.parse_condition(conditions)
        if not valid_condition:
            print("Error: Invalid condition.")
            return
        
        condition_index = []
        for ct in condition_targets:
            if ct in self.tables[table]:
                condition_index.append(self.tables[table].index(ct))
            else:
                print(f'Error: Column {ct} not in table {table}')
                return
        
        # Open the JSON file
        with open(f'sql_tables/{table}/metadata.json', 'r') as file:
            # Load JSON data from the file into a Python object
            metadata = json.load(file)
        for table_chunk_num in metadata:
            with open(f'sql_tables/{table}/table_{table_chunk_num}.csv', 'r') as csvfile:
                # Create a CSV reader object
                csvreader = csv.reader(csvfile)
                header = next(csvreader)
                rows = list(csvreader)
            new_rows_index = []
            for i,r in enumerate(rows):
                condition_input = []
                for cti in condition_index:
                    condition_input.append(r[cti])
                if self.check_condition(condition_input, condition_operators, condition_comparisons, condition_logics):
                    print(f'delete:{r} from table {table}')
                    metadata[table_chunk_num] -= 1
                else:
                    new_rows_index.append(i)
            rows = [rows[i] for i in new_rows_index]
            with open(f'sql_tables/{table}/table_{table_chunk_num}.csv', mode='w', newline='') as file:
                writer = csv.writer(file)
                # Write rows to the CSV file
                writer.writerow(header)
                for r in rows:
                    writer.writerow(r)
            # Open the JSON file
            with open(f'sql_tables/{table}/metadata.json', 'w') as file:
                # save the new metadata to metadata json
                json.dump(metadata, file, indent=4)
        self.fill_chunks(table)
    def update(self, table, values, conditions):
        #print(f"update values {values} from table {table} on condition {conditions}")
        if table not in self.tables:
            print(f"Table {table} doesn't exist.")
        
        if not values:
            print('Nothing to Change.')
            return
        
        if not conditions:
            print(f'Error: Need condition(ex. WHEN name = Jane) to Change data.')
            return
        
        #### parse condition (WHEN)
        condition_targets, condition_operators, condition_comparisons, condition_logics, valid_condition = self.parse_condition(conditions)
        if not valid_condition:
            print("Error: Invalid condition.")
            return
        condition_index = []
        for ct in condition_targets:
            if ct in self.tables[table]:
                condition_index.append(self.tables[table].index(ct))
            else:
                print(f'Error: Column {ct} not in table {table}')
                return
        
        ### parse values

        # check if there are value that is spereate by space ex. 'Justin Chen' -> ["'Justin" , "Chen'""]
        new_values = []
        need_new = False
        skip = 0
        for i, c in enumerate(values):
            if skip:
                skip-=1
                continue
            if c.startswith('"') or c.startswith("'"):
                new_sring = c[1:]+' '
                temp_i = i
                while not values[temp_i+1].endswith('"') and not values[temp_i+1].endswith("'"):
                    new_sring+=values[temp_i+1]
                    new_sring+=' '
                    temp_i+=1
                    skip+=1
                new_sring+=values[temp_i+1][:-1]
                skip+=1
                new_values.append(new_sring)
                need_new = True
            else:
                new_values.append(c)
        if need_new:
            values = new_values
        print(f"update values {values} from table {table} on condition {conditions}")

        values_change_map = {}
        # check values in correct format and valid, and get value update index
        if len(values) % 3 != 0:
            print("Error: incorrect value format (Correct format: name = 'Jane Chen', age = 25)")
            return

        for i in range(int(len(values)/3)):
            
            col = values[i*3]
            equal_sign =  values[i*3 + 1]
            val = values[i*3 + 2]

            if equal_sign != '=':
                print("Error: incorrect value format (Correct format: name = 'Jane Chen', age = 25)")
                return
            
            if col not in self.tables[table]:
                print(f'Error: Column {col} not in table {table}')
                return
            values_change_map[self.tables[table].index(col)] = val

        # Open the JSON file
        with open(f'sql_tables/{table}/metadata.json', 'r') as file:
            # Load JSON data from the file into a Python object
            metadata = json.load(file)
        for table_chunk_num in metadata:
            with open(f'sql_tables/{table}/table_{table_chunk_num}.csv', 'r') as csvfile:
                # Create a CSV reader object
                csvreader = csv.reader(csvfile)
                header = next(csvreader)
                rows = list(csvreader)
            
            need_change = False
            for i,r in enumerate(rows):
                condition_input = []
                for cti in condition_index:
                    condition_input.append(r[cti])
                if self.check_condition(condition_input, condition_operators, condition_comparisons, condition_logics):
                
                    new_r = r.copy()
                    for vi in values_change_map:
                        new_r[vi] = values_change_map[vi]
                    if not self.check_key(table, new_r.copy(),ignore=r.copy()):
                        print("Error: Value check doesn't pass, cannot update this value to table")
                        return
                    need_change = True
                    rows[i] = new_r
                    print(f'change:{r} from table {table} to {new_r}')
            if need_change:
                with open(f'sql_tables/{table}/table_{table_chunk_num}.csv', mode='w', newline='') as file:
                    writer = csv.writer(file)
                    # Write rows to the CSV file
                    writer.writerow(header)
                    for r in rows:
                        writer.writerow(r)

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
            if targets[i] == True: # pass this condition
                pass_condition = True
            elif op == "=":
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
            skip = 0
            need_new = False
            new_conditions = []
            for i, c in enumerate(conditions):
                if skip:
                    skip-=1
                    continue
                if c.startswith('"') or c.startswith("'"):
                    new_sring = c[1:]+' '
                    temp_i = i
                    while not conditions[temp_i+1].endswith('"') and not conditions[temp_i+1].endswith("'"):
                        new_sring+=conditions[temp_i+1]
                        new_sring+=' '
                        temp_i+=1
                        skip+=1
                    new_sring+=conditions[temp_i+1][:-1]
                    skip+=1
                    new_conditions.append(new_sring)
                    need_new = True
                else:
                    new_conditions.append(c)
            if need_new:
                conditions = new_conditions

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
    def connect_row(self, target_index, pre_header, pre_row, connect_table, on_condition, condition_target_index, condition_operators, condition_comparisons, condition_logics, aggregation, aggregation_values):
        table = connect_table.pop(0)
        left_c = on_condition.pop(0)
        on_condition.pop(0)
        right_c = on_condition.pop(0)
        left_t = None
        right_t = None
        if '.' in left_c:
            left_t = left_c.split('.')[0]
            left_c = left_c.split('.')[1]
        if '.' in right_c:
            right_t = right_c.split('.')[0]
            right_c = right_c.split('.')[1]
        if left_t:
            if left_t == table:
                temp = left_c
                left_c = right_c
                right_c = temp
        elif right_t:
            if right_t != table:
                temp = left_c
                left_c = right_c
                right_c = temp
        
        with open(f'sql_tables/{table}/metadata.json', 'r') as file:
            # Load JSON data from the file into a Python object
            metadata = json.load(file)

        for table_chunk_num in metadata:
            with open(f'sql_tables/{table}/table_{table_chunk_num}.csv', 'r') as csvfile:
                # Create a CSV reader object
                csvreader = csv.reader(csvfile)
                header = next(csvreader)
                rows = list(csvreader)
            for r in rows:
                if pre_row[pre_header.index(left_c)] == r[header.index(right_c)]:
                    row = pre_row+r
                    
                else:
                    continue
                if connect_table:
                    self.connect_row(target_index, pre_header+header, row, connect_table.copy(), on_condition.copy(), condition_target_index, condition_operators, condition_comparisons, condition_logics)
                else:
                    # WHEN check condition
                    if not self.check_condition([row[cti] for cti in condition_target_index], condition_operators, condition_comparisons, condition_logics):
                        continue
                    # check if aggregation
                    if aggregation:
                        if not self.prepare_aggregation(row, aggregation, aggregation_values, target_index):
                            print('Error: Cannot perform this aggregation.')
                            return
                    else:
                        values_to_print = []
                        for ti in target_index:
                            values_to_print.append(row[ti])
                        print(values_to_print)
    def create_group_tables(self, table, grouping_c):
        # split the table to different group table (each group one table)
        group_dict = {}
        group_table_info = {}
        unique_group = 0
        grouping_c_index = self.tables[table].index(grouping_c)
        #### delete grouping directory if appllicable
        # Specify the directory you want to delete
        directory_to_delete = "sql_grouping"
        # Use try-except block to handle exceptions
        try:
            os.mkdir('sql_grouping')
        except:
            shutil.rmtree(directory_to_delete)
            os.mkdir('sql_grouping')
        #os.mkdir('sql_grouping')
        os.mkdir(f'sql_grouping/{table}')
        with open(f'sql_tables/{table}/metadata.json', 'r') as file:
        # Load JSON data from the file into a Python object
            metadata = json.load(file)
        for table_chunk_num in metadata:
            with open(f'sql_tables/{table}/table_{table_chunk_num}.csv', 'r') as csvfile:
                # Create a CSV reader object
                csvreader = csv.reader(csvfile)
                header = next(csvreader)
                rows = list(csvreader)
            for r in rows:
                if r[grouping_c_index] not in group_dict:
                    group_dict[r[grouping_c_index]] = unique_group
                    group_table_info[unique_group] = 1
                    # Create and write to the CSV file
                    with open(f'sql_grouping/{table}/table_{unique_group}.csv', mode='w', newline='') as file:
                        writer = csv.writer(file)
                        writer.writerow(header)
                        writer.writerow(r)
                    unique_group+=1
                else:
                    group_num = group_dict[r[grouping_c_index]]
                    group_table_info[group_num] += 1
                    #write to the CSV file
                    with open(f'sql_grouping/{table}/table_{group_num}.csv', mode='a', newline='') as file:
                        writer = csv.writer(file)
                        writer.writerow(r)
        # Define the name of the JSON file
        json_filename = f'sql_grouping/{table}/metadata.json'
        # Open the file for writing
        with open(json_filename, 'w') as file:
            # Use json.dump() to write the data to a file
            json.dump(group_table_info, file, indent=4)  # 'indent=4' for pretty-printing

    def prepare_aggregation(self, row, aggregation, aggregation_values, target_index):
        for i, agg in enumerate(aggregation):
            agg_operator = agg[0]
            agg_target_i = agg[1]
            if target_index[agg_target_i] =='*':
                target_value = '*'
                if agg_operator != 'CNT':
                    print('Error: can only use CNT with * in aggregation' )
                    return False
            else:
                target_value = row[target_index[agg_target_i]]
            if target_value == '': ## NULL value
                return True
            if agg_operator == 'CNT':
                if aggregation_values[i] == None: # initialize value
                    aggregation_values[i] = 1
                else :
                    aggregation_values[i] += 1
            elif agg_operator == 'MX':
                if aggregation_values[i] == None: # initialize value
                    aggregation_values[i] = target_value
                else:
                    aggregation_values[i] = max(target_value,aggregation_values[i])
            elif agg_operator == 'MN':
                if aggregation_values[i] == None: # initialize value
                    aggregation_values[i] = target_value
                else:
                    aggregation_values[i] = min(target_value,aggregation_values[i])
            elif agg_operator == 'SM':
                if type(target_value) != int:
                    return False
                if aggregation_values[i] == None: # initialize value
                    aggregation_values[i] = target_value
                else:
                    aggregation_values[i] = target_value+aggregation_values[i]
            elif agg_operator == 'AVRG':
                if not (type(target_value) == int or type(target_value) == float):
                    return False
                if aggregation_values[i] == None: # initialize value
                    aggregation_values[i] = [target_value,1]
                else:
                    aggregation_values[i][0] += target_value
                    aggregation_values[i][1] += 1
        return True
        
    def output_aggregation(self, values_to_print, aggregation, aggregation_values):
        for i, agg in enumerate(aggregation):
            agg_operator = agg[0]
            agg_target_i = agg[1]
            if agg_operator == 'CNT':
                values_to_print[agg_target_i] = aggregation_values[i]
            elif agg_operator == 'MX':
                values_to_print[agg_target_i] = aggregation_values[i]
            elif agg_operator == 'MN':
                values_to_print[agg_target_i] = aggregation_values[i]
            elif agg_operator == 'SM':
                values_to_print[agg_target_i] = aggregation_values[i]
            elif agg_operator == 'AVRG':
                values_to_print[agg_target_i] = aggregation_values[i][0]/aggregation_values[i][1]
        return
    def sort_table(self, order_table, order_index, ordering):
        def sort_one_file(order_table,table_chunk_num,order_index,ordering):
            with open(f'sql_tables/{order_table}/table_{table_chunk_num}.csv', 'r') as csvfile:
                # Create a CSV reader object
                csvreader = csv.reader(csvfile)
                header = next(csvreader)
                rows = list(csvreader)
            unsorted_target = []
            for r in rows:
                unsorted_target.append(r[order_index])
            sorted_index = sorted(range(len(unsorted_target)), key=lambda i: unsorted_target[i],reverse=(ordering=='DSC'))
            with open(f'sql_tables/{order_table}/table_{table_chunk_num}.csv', 'w', newline='') as csvfile:
                # Create a writer object
                csvwriter = csv.writer(csvfile)
                csvwriter.writerow(header)
                for si in sorted_index:
                    # Write the new row
                    csvwriter.writerow(rows[si])
        def merge_blocks(left_blocks, right_blocks, order_table, order_index, ordering):
            file_nums = []
            for n in left_blocks:
                file_nums.append(n)
            for n in right_blocks:
                file_nums.append(n)
            unread_left_blocks = len(left_blocks)
            unread_right_blocks = len(right_blocks)
            with open(f'sql_tables/{order_table}/metadata.json', 'r') as file:
                # Load JSON data from the file into a Python object
                metadata = json.load(file)
            left_b = left_blocks.pop(0)
            right_b = right_blocks.pop(0)
            left_count = metadata[left_b]
            right_count = metadata[right_b]
            wrote_file_num = int(file_nums[0])
            wrote_count = 0
            wrote = []
            csvfile_l = open(f'sql_tables/{order_table}/table_{left_b}.csv', 'r')
            csvreader_l = csv.reader(csvfile_l)
            header = next(csvreader_l)
            csvfile_r = open(f'sql_tables/{order_table}/table_{right_b}.csv', 'r')
            csvreader_r = csv.reader(csvfile_r)
            next(csvreader_r)
            l = next(csvreader_l)
            r = next(csvreader_r)
            while(unread_left_blocks and unread_right_blocks):
                while(left_count and right_count):
                    if ordering == 'ASC':
                        if l[order_index] <= r[order_index]:
                            wrote.append(l)
                            left_count-=1
                            if left_count != 0:
                                l = next(csvreader_l)
                        else:
                            wrote.append(r)
                            right_count-=1
                            if right_count != 0:
                                r = next(csvreader_r)
                    else:
                        if l[order_index] >= r[order_index]:
                            wrote.append(l)
                            left_count-=1
                            if left_count != 0:
                                l = next(csvreader_l)
                        else:
                            wrote.append(r)
                            right_count-=1
                            if right_count != 0:
                                r = next(csvreader_r)
                    wrote_count+=1
                    if wrote_count == 2000:
                        with open(f'sql_tables/{order_table}/table_{wrote_file_num}_temp.csv', 'w', newline='') as csvfile:
                            # Create a writer object
                            csvwriter = csv.writer(csvfile)
                            csvwriter.writerow(header)
                            for w in wrote:
                                csvwriter.writerow(w)
                        wrote = []
                        wrote_file_num+=1
                        wrote_count = 0
                if left_count == 0:
                    unread_left_blocks -= 1
                    if unread_left_blocks > 0:
                        left_b = left_blocks.pop(0)
                        left_count = metadata[left_b]
                        csvfile_l = open(f'sql_tables/{order_table}/table_{left_b}.csv', 'r')
                        csvreader_l = csv.reader(csvfile_l)
                        next(csvreader_l)
                        l = next(csvreader_l)
                else:
                    unread_right_blocks -= 1
                    if unread_right_blocks > 0:
                        right_b = right_blocks.pop(0)
                        right_count = metadata[right_b]
                        csvfile_r = open(f'sql_tables/{order_table}/table_{right_b}.csv', 'r')
                        csvreader_r = csv.reader(csvfile_r)
                        next(csvreader_r)
                        r = next(csvreader_r)
            while(unread_left_blocks != 0):
                while(left_count):
                    wrote.append(l)
                    left_count-=1
                    if left_count != 0:
                        l = next(csvreader_l)
                    wrote_count+=1
                    if wrote_count == 2000:
                        with open(f'sql_tables/{order_table}/table_{wrote_file_num}_temp.csv', 'w', newline='') as csvfile:
                            # Create a writer object
                            csvwriter = csv.writer(csvfile)
                            csvwriter.writerow(header)
                            for w in wrote:
                                csvwriter.writerow(w)
                        wrote = []
                        wrote_file_num+=1
                        wrote_count = 0
                unread_left_blocks -= 1
                if unread_left_blocks > 0:
                    left_b = left_blocks.pop(0)
                    left_count = metadata[left_b]
                    csvfile_l = open(f'sql_tables/{order_table}/table_{left_b}.csv', 'r')
                    csvreader_l = csv.reader(csvfile_l)
                    next(csvreader_l)
                    l = next(csvreader_l)
            while(unread_right_blocks != 0):
                while(right_count):
                    wrote.append(r)
                    right_count-=1
                    if right_count != 0:
                        r = next(csvreader_r)
                    wrote_count+=1
                    if wrote_count == 2000:
                        with open(f'sql_tables/{order_table}/table_{wrote_file_num}_temp.csv', 'w', newline='') as csvfile:
                            # Create a writer object
                            csvwriter = csv.writer(csvfile)
                            csvwriter.writerow(header)
                            for w in wrote:
                                csvwriter.writerow(w)
                        wrote = []
                        wrote_file_num+=1
                        wrote_count = 0
                unread_right_blocks -= 1
                if unread_right_blocks > 0:
                    right_b = right_blocks.pop(0)
                    right_count = metadata[right_b]
                    csvfile_r = open(f'sql_tables/{order_table}/table_{right_b}.csv', 'r')
                    csvreader_r = csv.reader(csvfile_r)
                    next(csvreader_r)
                    r = next(csvreader_r)
            if wrote_count != 0:
                with open(f'sql_tables/{order_table}/table_{wrote_file_num}_temp.csv', 'w', newline='') as csvfile:
                    # Create a writer object
                    csvwriter = csv.writer(csvfile)
                    csvwriter.writerow(header)
                    for w in wrote:
                        csvwriter.writerow(w)
            for f in file_nums:
                os.remove(f'sql_tables/{order_table}/table_{f}.csv')
                os.rename(f'sql_tables/{order_table}/table_{f}_temp.csv', f'sql_tables/{order_table}/table_{f}.csv')

        with open(f'sql_tables/{order_table}/metadata.json', 'r') as file:
            # Load JSON data from the file into a Python object
            metadata = json.load(file)
        sorted_blocks = []
        new_sorted_blocks = []
        block_num = len(metadata)
        unsorted_num = len(metadata)

        # Merge sort
        # step 1: sort each chunk of first
        for table_chunk_num in metadata:
            sort_one_file(order_table,table_chunk_num,order_index,ordering)
            sorted_blocks.append([table_chunk_num])
        # step 2 merge each chunk
        new_sorted_blocks = sorted_blocks.copy()
        while len(new_sorted_blocks) > 1:
            new_sorted_blocks = []
            while len(sorted_blocks) > 1:
                left = sorted_blocks.pop(0)
                right = sorted_blocks.pop(0)
                merge_blocks(left.copy(),right.copy(),order_table,order_index,ordering)
                new_sorted_blocks.append(left+right)
            if len(sorted_blocks) == 1:
                new_sorted_blocks.append(sorted_blocks[0])
            sorted_blocks = new_sorted_blocks.copy()
    def get(self, table, columns, connect_table=None, on_condition=None, conditions=None, grouping=None, ordering=None, order_by=None):
        print(f"output columns {columns} from table {table} connect with table {connect_table} on {on_condition} with conditions {conditions} gather by {grouping} order by {order_by} in {ordering} order")
        #### parse condition (WHEN)
        condition_targets, condition_operators, condition_comparisons, condition_logics, valid_condition = self.parse_condition(conditions)
        if not valid_condition:
            print("Error: Invalid condition.")
            return
        
        # Check Ordering
        order_table = None
        order_index = None
        if order_by:
            order_by = order_by[0]
            if '.' not in order_by:
                if order_by in self.tables[table]:
                    order_table = table
                    order_index = self.tables[table].index(order_by)
                else:
                    if connect_table:
                        for ct in connect_table:
                            if order_by in self.tables[ct]:
                                order_table = ct
                                order_index = self.tables[ct].index(order_by)
            else:
                order_t = order_by.split('.')[0]
                order_c = order_by.split('.')[1]
                if order_t == table or (connect_table and order_t in connect_table):
                    order_table = order_t
                    order_index = self.tables[order_t].index(order_c)
            if order_table == None:
                print('Error: Invalid Ordering column or table.')
                return
            self.sort_table(order_table, order_index, ordering)
        
        #### check grouping 
        if grouping:
            if '.' in grouping[0]:
                grouping_t = grouping[0].split('.')[0]
                grouping_c = grouping[0].split('.')[1]
                if grouping_t not in self.tables:
                    print("Error: Invalid grouping table.")
                    return
                else:
                    if grouping_c not in self.tables[grouping_t]:
                        print("Error: Invalid grouping column.")
                        return
                
                if grouping_t != table:
                    if connect_table and grouping_t in connect_table: # swap the first table to the grouping table
                        # swap table
                        gt_i = connect_table.index(grouping_t)
                        connect_table[gt_i] = table
                        table = grouping_t
                        # swap on condition
                        temp = on_condition[3*gt_i]
                        on_condition[3*gt_i] = on_condition[3*gt_i+2]
                        on_condition[3*gt_i+2] = temp
                    else:
                        print("Error: Invalid grouping table.")
                        return
                    
                self.create_group_tables(table,grouping_c)

            else:
                print('Error: Wrong grouping format: table_name.column_name')
                return

        ##### Projection
        # check table name and columns
        tables_cols = []
        tables_cols_with_name = []
        if table not in self.tables:
            print(f"Table {table} doesn't exist.")
            return
        else:
            tables_cols+=self.tables[table]
            for t in self.tables[table]:
                tables_cols_with_name.append(table+'.'+t)
        if connect_table:
            for ct in connect_table:
                if ct not in self.tables:
                    print(f"Table {table} to connect doesn't exist.")
                    return
                else:
                    tables_cols+=self.tables[ct]
                    for t in self.tables[ct]:
                        tables_cols_with_name.append(ct+'.'+t)
        target_index = []
        aggregation = []
        aggr_c = False
        normal_c = False
        for i, col in enumerate(columns):
            if re.search("(^CNT\(|^MX\(|^MN\(|^SM\(|^AVRG\().+\)$",col):
                if not grouping:
                   print('Error: cannot perform aggregation without grouping (GATHER_BY)')
                   return
                if normal_c:
                    print('Error: cannot project normal column with aggragation except the grouping column')
                    return
                brak_i = col.index('(')
                c = col[brak_i+1:-1]
                aggregation.append((col[:brak_i],i))
                aggr_c = True
            else:
                c = col
                if grouping:
                    if not (c == grouping_c or c == f'{grouping_t}.{grouping_c}'):
                        if aggr_c:
                            print('Error: cannot project normal column with aggragation except the grouping column')
                            return
                        normal_c = True
                
            if '.' in c: ## ex Customer.name
                if c not in tables_cols_with_name: 
                    print(f"Column {c} doesn't exist or unable to get.")
                    return
                else:
                    target_index.append(tables_cols_with_name.index(c))
            else:
                if c not in tables_cols:
                    if c == '*':
                        if aggr_c:
                            target_index.append('*')
                        else: 
                            if len(columns) != 1:
                                print('Error: Cannot project * and other column at the same time. only * is needed.')
                                return
                            target_index = [tc for tc in range(len(tables_cols))]
                            columns = [tc for tc in tables_cols]
                            break
                    else:
                        print(f"Column {c} doesn't exist or unable to get.")
                        return
                else:
                    target_index.append(tables_cols.index(c))

        condition_target_index = []
        if condition_targets:
            for ct in condition_targets:
                if '.' in ct: ## exustomer.name
                    if ct not in tables_cols_with_name:
                        print(f"Column {ct} doesn't exist or unable to get.")
                        return
                    else:
                        condition_target_index.append(tables_cols_with_name.index(ct))
                else:
                    if ct not in tables_cols:
                        print(f"Column {ct} doesn't exist or unable to get.")
                        return
                    else:
                        condition_target_index.append(tables_cols.index(ct))
        
        # Open the metadata JSON file
        table_dir = 'sql_tables'
        if grouping:
            table_dir = 'sql_grouping'
        
        with open(f'{table_dir}/{table}/metadata.json', 'r') as file:
            # Load JSON data from the file into a Python object
            metadata = json.load(file)
        column_print=f'|{" | ".join(columns)}|'
        print(column_print)
        print('-'*len(column_print))
        for table_chunk_num in metadata:
            aggregation_values = [None]*len(aggregation)
            with open(f'{table_dir}/{table}/table_{table_chunk_num}.csv', 'r') as csvfile:
                # Create a CSV reader object
                csvreader = csv.reader(csvfile)
                header = next(csvreader)
                rows = list(csvreader)
            for row in rows:
                # WHEN check condition
                condition_input = []
                for cti in condition_target_index:
                    if cti > len(row)-1:
                        condition_input.append(True)
                    else:
                        condition_input.append(row[cti])
                if not self.check_condition(condition_input, condition_operators, condition_comparisons, condition_logics):
                    continue
                if connect_table:
                    self.connect_row(target_index, header, row, connect_table.copy(), on_condition.copy(), condition_target_index, condition_operators, condition_comparisons, condition_logics, aggregation, aggregation_values)
                else:
                    # WHEN check condition
                    if not self.check_condition([row[cti] for cti in condition_target_index], condition_operators, condition_comparisons, condition_logics):
                        continue
                    # check if aggregation
                    if aggr_c:
                        if not self.prepare_aggregation(row, aggregation, aggregation_values, target_index):
                            print('Error: Cannot perform this aggregation.')
                            return
                    else:
                        values_to_print = []
                        for ti in target_index:
                            values_to_print.append(row[ti])
                        print(values_to_print)
            if aggr_c:
                values_to_print = []
                for ti in target_index:
                    if ti == '*':
                        values_to_print.append('*')
                    else:
                        values_to_print.append(row[ti])
                self.output_aggregation(values_to_print, aggregation, aggregation_values)
                print(values_to_print)
        #### delete grouping directory if appllicable
        # Specify the directory you want to delete
        directory_to_delete = "sql_grouping"
        # Use try-except block to handle exceptions
        try:
            shutil.rmtree(directory_to_delete)
            #print(f"The directory {directory_to_delete} has been deleted successfully.")
        except OSError as e:
            pass
            #$print(f"Error: {e.strerror}")

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
                    if group_by_field != None:
                        group_value = get_nested_value(record, group_by_field.split('.')) if group_by_field else 'default'
                        grouped_records[group_value].append(record)

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
