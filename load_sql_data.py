import csv
import json
load_customer = False
load_ticket = True

def load_all():
    # write to customer tables
    # Open the CSV file
    with open('dataset/customer_support_ticket_dataset/customer_support_tickets.csv', 'r') as csvfile:
        # Create a CSV reader object
        csvreader = csv.reader(csvfile)
        next(csvreader)  # Skip the first row
        table_num = -1
        customer_table_info = {}
        customer_unique = []
        customer_id_count = 0
        # Iterate over each row in the CSV file
        for row in csvreader:
            
            if customer_id_count%2000 == 0:
                if customer_id_count!=0:
                    # Create and write to the CSV file
                    with open(table_csv_filename, mode='w', newline='') as file:
                        writer = csv.writer(file)
                        customer_table_info[table_num] = len(rows)-1
                        # Write rows to the CSV file
                        for r in rows:
                            writer.writerow(r)
                
                table_num += 1
                table_csv_filename = f'sql_tables/Customer/table_{table_num}.csv'
                # Define the table columns
                rows = [
                    ["customer_id", "name", "email", "age", "gender"]
                ]
            
            # extract the column we need
            extracted_columns = [row[x] for x in [1,2,3,4]] #name, email, age, gender
            # check if the customer already read
            if extracted_columns[1] not in customer_unique:
                customer_id_count+=1
                customer_unique.append(extracted_columns[1])
                rows.append([customer_id_count]+extracted_columns)
        # Create and write to the CSV file
        with open(table_csv_filename, mode='w', newline='') as file:
            writer = csv.writer(file)
            customer_table_info[table_num] = len(rows)-1
            # Write rows to the CSV file
            for row in rows:
                writer.writerow(row)
    # Define the name of the JSON file
    json_filename = 'sql_tables/Customer/metadata.json'
    # Open the file for writing
    with open(json_filename, 'w') as file:
        # Use json.dump() to write the data to a file
        json.dump(customer_table_info, file, indent=4)  # 'indent=4' for pretty-printing

    # write to product tables
    # Open the CSV file
    with open('dataset/customer_support_ticket_dataset/customer_support_tickets.csv', 'r') as csvfile:
        # Create a CSV reader object
        csvreader = csv.reader(csvfile)
        next(csvreader)  # Skip the first row
        table_num = -1
        product_table_info = {}
        product_unique = []
        product_id_count = 0
        # Iterate over each row in the CSV file
        for row in csvreader:
            if product_id_count%2000 == 0:
                if product_id_count!=0:
                    # Create and write to the CSV file
                    with open(table_csv_filename, mode='w', newline='') as file:
                        writer = csv.writer(file)
                        product_table_info[table_num] = len(rows)-1
                        # Write rows to the CSV file
                        for r in rows:
                            writer.writerow(r)
                
                table_num += 1
                table_csv_filename = f'sql_tables/Product/table_{table_num}.csv'
                # Define the table columns
                rows = [
                    ["product_id", "p_name"]
                ]
            
            # extract the column we need
            product_name = row[5] #product purchased
            # check if the customer already read
            if product_name not in product_unique:
                product_id_count+=1
                product_unique.append(product_name)
                rows.append([product_id_count]+[product_name])
        # Create and write to the CSV file
        with open(table_csv_filename, mode='w', newline='') as file:
            writer = csv.writer(file)
            product_table_info[table_num] = len(rows)-1
            # Write rows to the CSV file
            for row in rows:
                writer.writerow(row)
            # Define the name of the JSON file
    json_filename = 'sql_tables/Product/metadata.json'
    # Open the file for writing
    with open(json_filename, 'w') as file:
        # Use json.dump() to write the data to a file
        json.dump(product_table_info, file, indent=4)  # 'indent=4' for pretty-printing


    # write to ticket tables
    # Open the CSV file
    with open('dataset/customer_support_ticket_dataset/customer_support_tickets.csv', 'r') as csvfile:
        # Create a CSV reader object
        csvreader = csv.reader(csvfile)
        next(csvreader)  # Skip the first row
        table_num = -1
        ticket_table_info = {}
        # Iterate over each row in the CSV file
        for i, row in enumerate(csvreader):
            
            if i%2000 == 0:
                if i!=0:
                    # Create and write to the CSV file
                    with open(table_csv_filename, mode='w', newline='') as file:
                        writer = csv.writer(file)
                        ticket_table_info[table_num] = len(rows)-1
                        # Write rows to the CSV file
                        for r in rows:
                            writer.writerow(r)
                
                table_num += 1
                table_csv_filename = f'sql_tables/Ticket/table_{table_num}.csv'
                # Define the table columns
                rows = [
                    ["ticket_id", "customer_id", "product_id", "date_of_purchase", "ticket_type", "ticket_subject", "ticket_description", "ticket_status", "resolution", "priority", "channel", "first_response_time", "time_to_resolution", "rating"]
                ]
            
            # extract the column we need
            extracted_columns = [row[x] for x in [2,5,6,7,8,9,10,11,12,13,14,15,16]] #.....

            cust_email = extracted_columns[0]
            # get the customer_id
            for id,eml in enumerate(customer_unique):
                if cust_email==eml:
                    extracted_columns[0] = id+1
            
            product_name = extracted_columns[1]
            # get the product_id
            for id,prod in enumerate(product_unique):
                if product_name==prod:
                    extracted_columns[1] = id+1

            rows.append([i+1]+extracted_columns)
        # Create and write to the CSV file
        with open(table_csv_filename, mode='w', newline='') as file:
            writer = csv.writer(file)
            ticket_table_info[table_num] = len(rows)-1
            # Write rows to the CSV file
            for row in rows:
                writer.writerow(row)
    json_filename = 'sql_tables/Ticket/metadata.json'
    # Open the file for writing
    with open(json_filename, 'w') as file:
        # Use json.dump() to write the data to a file
        json.dump(ticket_table_info, file, indent=4)  # 'indent=4' for pretty-printing

    #nosql
    input_file_path = 'roam_prescription_based_prediction.jsonl'
    output_file_path = 'first_2000.json'

    # Open the input file and read the JSON objects line by line
    records = []
    with open(input_file_path, 'r') as file:
        try:
            while len(records) < 2000:
                line = next(file)  # Read the next line
                # Convert line into a JSON object and append to records list
                record = json.loads(line)
                records.append(record['cms_prescription_counts'])
                records.append(record['npi'])
                records.append(record['provider_variables'])
        except StopIteration:
            # Reached the end of the file before reading 2000 records
            pass
        except json.JSONDecodeError as e:
            print(f"An error occurred while parsing JSON: {e}")

    # Write the first 2000 'provider_variables' records to a new file
    with open(output_file_path, 'w') as file:
        json.dump(records, file, indent=4)

    def read_chunk_json_objects(file_path, start_record, end_record):
        # Read the entire file into a single string
        with open(file_path, 'r') as file:
            data = file.read()

        # Use a try-except block to catch JSON decoding errors
        try:
            # Assuming each object is separated by a newline
            # Create a generator to yield JSON objects instead of loading them all at once
            objects_generator = (json.loads(obj) for obj in data.split('\n') if obj.strip())

            # Skip records until we reach the start_record
            for _ in range(start_record):
                next(objects_generator, None)  # Advance the generator

            # Take records from start_record to end_record
            selected_records = [next(objects_generator, None) for _ in range(end_record - start_record)]

            # Clean the list to remove any 'None' values in case we have less than end_record objects
            selected_records = [record for record in selected_records if record is not None]

            return selected_records
        except json.JSONDecodeError as e:
            print(f"An error occurred: {e}")
            return None

    # Replace 'your_file.json' with the path to your JSON file
    json_objects = read_chunk_json_objects('roam_prescription_based_prediction.jsonl', 2000, 4000)

    # Now that we have the records, save them to a new JSON file
    if json_objects:
        with open('records_2000_to_4000.json', 'w') as outfile:
            json.dump(json_objects, outfile, indent=4)

    def read_chunk_json_objects(file_path, start_record, end_record):
        # Read the entire file into a single string
        with open(file_path, 'r') as file:
            data = file.read()

        # Use a try-except block to catch JSON decoding errors
        try:
            # Assuming each object is separated by a newline
            # Create a generator to yield JSON objects instead of loading them all at once
            objects_generator = (json.loads(obj) for obj in data.split('\n') if obj.strip())

            # Skip records until we reach the start_record
            for _ in range(start_record):
                next(objects_generator, None)  # Advance the generator

            # Take records from start_record to end_record
            selected_records = [next(objects_generator, None) for _ in range(end_record - start_record)]

            # Clean the list to remove any 'None' values in case we have less than end_record objects
            selected_records = [record for record in selected_records if record is not None]

            return selected_records
        except json.JSONDecodeError as e:
            print(f"An error occurred: {e}")
            return None


    # Replace 'your_file.json' with the path to your JSON file
    json_objects = read_chunk_json_objects('roam_prescription_based_prediction.jsonl', 4000, 6000)

    # Now that we have the records, save them to a new JSON file
    if json_objects:
        with open('records_4000_to_6000.json', 'w') as outfile:
            json.dump(json_objects, outfile, indent=4)




