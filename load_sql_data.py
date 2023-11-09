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

    #cms_prescription_counts 0-2000
    output_file_path = 'first_2000_cms_prescription_counts.json'
    # Open the input file and read the JSON objects line by line
    records = []
    with open(input_file_path, 'r') as file:
        try:
            while len(records) < 2000:
                line = next(file)  # Read the next line
                # Convert line into a JSON object and append to records list
                record = json.loads(line)
                records.append(record['cms_prescription_counts'])
        except StopIteration:
            # Reached the end of the file before reading 2000 records
            pass
        except json.JSONDecodeError as e:
            print(f"An error occurred while parsing JSON: {e}")
    # Write the first 2000 'cms_prescription_counts' records to a new file
    with open(output_file_path, 'w') as file:
        json.dump(records, file, indent=4)

    #cms_prescription_counts 2000-4000
    output_file_path = '2000_to_4000_cms_prescription_counts.json'
    records_to_extract = 2000  # Number of records to extract
    start_record_index = 2000  # Starting index, zero-based
    # Open the input file and read the JSON objects line by line
    extracted_records = []
    with open(input_file_path, 'r') as file:
        for _ in range(start_record_index):
             # Skip the first 2000 records
            next(file)
        # Process the next 2000 records
        for _ in range(records_to_extract):
            try:
                line = next(file)  # Read the next line
                record = json.loads(line)  # Parse the JSON data from the line
                extracted_records.append(record['cms_prescription_counts'])
            except StopIteration:
                # End of file reached
                break
            except json.JSONDecodeError as e:
                print(f"An error occurred while parsing JSON: {e}")
                break
    # Write the 2000 to 4000 'cms_prescription_counts' records to a new file
    with open(output_file_path, 'w') as file:
        json.dump(extracted_records, file, indent=4)

    #cms_prescription_counts 4000-6000
    output_file_path = '4000_to_6000_cms_prescription_counts.json'
    records_to_extract = 2000  # Number of records to extract
    start_record_index = 4000  # Starting index, zero-based
    # Open the input file and read the JSON objects line by line
    extracted_records = []
    with open(input_file_path, 'r') as file:
        for _ in range(start_record_index):
            # Skip the first 2000 records
            next(file)
        # Process the next 2000 records
        for _ in range(records_to_extract):
            try:
                line = next(file)  # Read the next line
                record = json.loads(line)  # Parse the JSON data from the line
                extracted_records.append(record['cms_prescription_counts'])
            except StopIteration:
                # End of file reached
                break
            except json.JSONDecodeError as e:
                print(f"An error occurred while parsing JSON: {e}")
                break
    # Write the 4000 to 6000 'cms_prescription_counts' records to a new file
    with open(output_file_path, 'w') as file:
        json.dump(extracted_records, file, indent=4)

    #npi 0-2000
    output_file_path = 'first_2000_npi.json'
    # Open the input file and read the JSON objects line by line
    records = []
    with open(input_file_path, 'r') as file:
        try:
            while len(records) < 2000:
                line = next(file)  # Read the next line
                # Convert line into a JSON object and append to records list
                record = json.loads(line)
                records.append(record['npi'])
        except StopIteration:
            # Reached the end of the file before reading 2000 records
            pass
        except json.JSONDecodeError as e:
            print(f"An error occurred while parsing JSON: {e}")
    # Write the first 2000 'npi' records to a new file
    with open(output_file_path, 'w') as file:
        json.dump(records, file, indent=4)

    #npi 2000-4000
    output_file_path = '2000_to_4000_npi.json'
    records_to_extract = 2000  # Number of records to extract
    start_record_index = 2000  # Starting index, zero-based
    # Open the input file and read the JSON objects line by line
    extracted_records = []
    with open(input_file_path, 'r') as file:
        for _ in range(start_record_index):
            # Skip the first 2000 records
            next(file)
        # Process the next 2000 records
        for _ in range(records_to_extract):
            try:
                line = next(file)  # Read the next line
                record = json.loads(line)  # Parse the JSON data from the line
                extracted_records.append(record['npi'])
            except StopIteration:
                # End of file reached
                break
            except json.JSONDecodeError as e:
                print(f"An error occurred while parsing JSON: {e}")
                break
    # Write the 2000 to 4000 'npi' records to a new file
    with open(output_file_path, 'w') as file:
        json.dump(extracted_records, file, indent=4)

    #npi 4000-6000
    output_file_path = '4000_to_6000_npi.json'
    records_to_extract = 2000  # Number of records to extract
    start_record_index = 4000  # Starting index, zero-based
    # Open the input file and read the JSON objects line by line
    extracted_records = []
    with open(input_file_path, 'r') as file:
        for _ in range(start_record_index):
            # Skip the first 2000 records
            next(file)
        # Process the next 2000 records
        for _ in range(records_to_extract):
            try:
                line = next(file)  # Read the next line
                record = json.loads(line)  # Parse the JSON data from the line
                extracted_records.append(record['npi'])
            except StopIteration:
                # End of file reached
                break
            except json.JSONDecodeError as e:
                print(f"An error occurred while parsing JSON: {e}")
                break
    # Write the 4000 to 6000 'npi' records to a new file
    with open(output_file_path, 'w') as file:
        json.dump(extracted_records, file, indent=4)

    #provider_variables 0-2000
    output_file_path = 'first_2000_provider_variables.json'
    # Open the input file and read the JSON objects line by line
    records = []
    with open(input_file_path, 'r') as file:
        try:
            while len(records) < 2000:
                line = next(file)  # Read the next line
                # Convert line into a JSON object and append to records list
                record = json.loads(line)
                records.append(record['provider_variables'])
        except StopIteration:
            # Reached the end of the file before reading 2000 records
            pass
        except json.JSONDecodeError as e:
            print(f"An error occurred while parsing JSON: {e}")
    # Write the first 2000 'provider_variables' records to a new file
    with open(output_file_path, 'w') as file:
        json.dump(records, file, indent=4)

    #provider_variables 2000-4000
    output_file_path = '2000_to_4000_provider_variables.json'
    records_to_extract = 2000  # Number of records to extract
    start_record_index = 2000  # Starting index, zero-based
    # Open the input file and read the JSON objects line by line
    extracted_records = []
    with open(input_file_path, 'r') as file:
        for _ in range(start_record_index):
            # Skip the first 2000 records
            next(file)
        # Process the next 2000 records
        for _ in range(records_to_extract):
            try:
                line = next(file)  # Read the next line
                record = json.loads(line)  # Parse the JSON data from the line
                extracted_records.append(record['provider_variables'])
            except StopIteration:
                # End of file reached
                break
            except json.JSONDecodeError as e:
                print(f"An error occurred while parsing JSON: {e}")
                break
    # Write the 2000 to 4000 'provider_variables' records to a new file
    with open(output_file_path, 'w') as file:
         json.dump(extracted_records, file, indent=4)

    #provider_variables 4000-6000
    output_file_path = '4000_to_6000_provider_variables.json'
    records_to_extract = 2000  # Number of records to extract
    start_record_index = 4000  # Starting index, zero-based
    # Open the input file and read the JSON objects line by line
    extracted_records = []
    with open(input_file_path, 'r') as file:
        for _ in range(start_record_index):
            # Skip the first 2000 records
            next(file)
        # Process the next 2000 records
        for _ in range(records_to_extract):
            try:
                line = next(file)  # Read the next line
                record = json.loads(line)  # Parse the JSON data from the line
                extracted_records.append(record['provider_variables'])
            except StopIteration:
                # End of file reached
                break
            except json.JSONDecodeError as e:
                print(f"An error occurred while parsing JSON: {e}")
                break
    # Write the 4000 to 6000 'provider_variables' records to a new file
    with open(output_file_path, 'w') as file:
        json.dump(extracted_records, file, indent=4)



load_all()
