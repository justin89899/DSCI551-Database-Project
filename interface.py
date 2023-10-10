
from database import Database

def main():
    db = Database()

    while True:
        query = input("Enter a query (or 'exit' to quit): ").strip().replace(",","")
        if query.lower() == 'exit':
            break

        try:
            if query.startswith("GET"):
                # Parse and execute GET statement
                # Example: GET id, name FROM users
                parts = query.split(" ")
                from_index = parts.index("FROM")
                table_name = parts[from_index+1]
                columns = parts[1:from_index]
                conditions = None
                if "WHEN" in parts:
                    when_index = parts.index("WHEN")
                    conditions = parts[when_index + 1:]

                db.get(table_name, columns, conditions)

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
