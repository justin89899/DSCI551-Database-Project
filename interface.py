
from database import *

def main():
    while True:
        # choose database
        databases = ["SQL", "noSQL"]
        db_to_use = input("Choose a database to use (SQL, noSQL) or exit:")
        if db_to_use not in databases:
            if db_to_use == 'exit':
                break
            print("not a existing database")
            continue
        if db_to_use=="SQL":
            db = SQL_Database()
        else:
            db = noSQL_Database()

        key_words = ["GET","FROM","CONNECT","WHEN","GATHER_BY","ASCEND_BY","DESCEND_BY","PUT","DROP","CHANGE"]
        while True:
            
            query = input("Enter a query (or 'exit' to quit): ").strip()
            if query.lower() == 'exit':
                break
            # get key words index
            parts = query.split(" ")
            key_words_index = []
            for i,k in enumerate(parts):
                if k in key_words:
                    key_words_index.append(i)
            key_words_index.append("end")
            try:
                if query.startswith("GET"):
                    # Parse and execute GET statement
                    # Example: GET id, name FROM users
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
                        #on_condition = parts[connect_index + 3:connect_index + 6]
                        if key_words_index[key_words_index.index(connect_index)+1] == "end":
                            on_condition = parts[connect_index + 3:]
                        else:
                            on_condition = parts[connect_index + 3:key_words_index[key_words_index.index(connect_index)+1]]
                    # parse condition (WHEN)
                    conditions = None
                    if "WHEN" in parts:
                        when_index = parts.index("WHEN")
                        if key_words_index[key_words_index.index(when_index)+1] == "end":
                            conditions = parts[when_index + 1:]
                        else:
                            conditions = parts[when_index + 1:key_words_index[key_words_index.index(when_index)+1]]
                    
                    # parse grouping (GATHER BY)    
                    grouping = None
                    if "GATHER_BY" in parts:
                        group_index = parts.index("GATHER_BY")
                        if key_words_index[key_words_index.index(group_index)+1] == "end":
                            grouping = parts[group_index + 1:]
                        else:
                            grouping = parts[group_index + 1:key_words_index[key_words_index.index(group_index)+1]]

                    # parse ordering (ASCEND_BY/DESCEND_BY)    
                    ordering = None
                    order_by = None
                    if "ASCEND_BY" in parts:
                        ordering_index = parts.index("ASCEND_BY")
                        ordering = "ASC"
                        if key_words_index[key_words_index.index(ordering_index)+1] == "end":
                            order_by = parts[ordering_index + 1:]
                        else:
                            order_by = parts[ordering_index + 1:key_words_index[key_words_index.index(ordering_index)+1]]
                    elif "DESCEND_BY" in parts:
                        ordering_index = parts.index("DESCEND_BY")
                        ordering = "DSC"
                        if key_words_index[key_words_index.index(ordering_index)+1] == "end":
                            order_by = parts[ordering_index + 1:]
                        else:
                            order_by = parts[ordering_index + 1:key_words_index[key_words_index.index(ordering_index)+1]]
                        
                    db.get(tables, columns, connect_table, on_condition, conditions, grouping, ordering, order_by)
                
                elif query.startswith("PUT"):
                    # Parse and execute PUT IN statement
                    # Example: PUT 1, 'Alice' IN users
                    IN_index = parts.index("IN")
                    table_name = parts[IN_index+1:]
                    # parse all the values
                    values = [i.replace(",", "") for i in parts[1:IN_index]]
                    db.insert(table_name, values)

                elif query.startswith("DROP"):
                    # Parse and execute DROP sth FROM table WHEN statement
                    # Example: DROP name FROM table WHEN id = 100
                    FROM_index = parts.index("FROM")
                    conditions = None
                    if "WHEN" in parts:
                        WHEN_index = parts.index("WHEN")
                        table_name = parts[FROM_index+1:WHEN_index]
                        conditions = parts[WHEN_index+1:]
                    # Parse the items
                    items = [i.replace(",", "") for i in parts[1:FROM_index]]

                    db.delete(table_name, items, conditions)

                elif query.startswith("CHANGE"):
                    # Parse and execute CHANGE table WITH values WHEN
                    # Example: CHANGE users WITH name = steve
                    WITH_index = parts.index("WITH")
                    table_name = parts[1]
                    WHEN_index = parts.index("WHEN")
                    values = [i.replace(",", "") for i in parts[WITH_index + 1: WHEN_index]]
                    conditions = parts[WHEN_index+1:]
                    db.update(table_name, values, conditions)
                
                else:
                    print("Invalid query.")

            except Exception as e:
                print(f"Error: {str(e)}")

if __name__ == "__main__":
    main()
