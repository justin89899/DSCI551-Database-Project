
from database import Database

def main():
    db = Database()
    key_words = ["GET","FROM","CONNECT","WHEN","GATHER","ASCEND","DESCEND","PUT","DROP","CHANGE"]
    while True:
        query = input("Enter a query (or 'exit' to quit): ").strip()
        if query.lower() == 'exit':
            break

        try:
            if query.startswith("GET"):
                # Parse and execute GET statement
                # Example: GET id, name FROM users
                parts = query.split(" ")
                from_index = parts.index("FROM")

                # parse all the columns
                columns = [i.replace(",","") for i in parts[1:from_index]]

                # parse all the tables
                i = 0
                tables = []
                while parts[from_index+1+i]:

                    tables.append(parts[from_index+1+i])
                    if parts[from_index+1+i].endswith(","):
                        i = i + 1
                    else:
                        break
                
                # parse join (CONNECT)
                connect_table = None
                if "CONNECT" in parts:
                    connect_index = parts.index("CONNECT")
                    connect_table = parts[connect_index + 1]
                    on_condition = parts[connect_index + 3:connect_index + 6]
                # parse condition
                conditions = None
                if "WHEN" in parts:
                    when_index = parts.index("WHEN")
                    conditions = parts[when_index + 1:]

                db.get(tables, columns, connect_table, on_condition, conditions)

                #for row in result:
                    #print(row)
            # elif query.startswith("CREATE TABLE"):
            #     # Parse and execute CREATE TABLE statement
            #     # Example: CREATE TABLE users (id INT, name TEXT)
            #     parts = query.split(" ")
            #     table_name = parts[2]
            #     columns = parts[4].split(",")
            #     db.create_table(table_name, [col.strip() for col in columns])

            # elif query.startswith("INSERT INTO"):
            #     # Parse and execute INSERT INTO statement
            #     # Example: INSERT INTO users VALUES (1, 'Alice')
            #     parts = query.split(" ")
            #     table_name = parts[2]
            #     values = eval("[" + query.split("VALUES")[1].strip() + "]")
            #     db.insert(table_name, values)
            else:
                print("Invalid query.")

        except Exception as e:
            print(f"Error: {str(e)}")

if __name__ == "__main__":
    main()
