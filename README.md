# DSCI551-Database-Project'

# Project Desciption:
This  project aimed at designing and implementing two database systems - one relational and the other NoSQL. Our objective was to explore the capabilities and performance characteristics of these systems under a common query language framework

# Directory Structure:
```
$ ls
README.md               database.py             interface.py            nosql_tables
__pycache__             dataset                 load_sql_data.py        sql_tables
```
### database.py
this file handles all the physical and logical operation of queries.

### interface.py
this file is for user interaction interface. Inputing queries.

### nosql_tables/
stores the data of noSQL database.

### sql_tables/
stores the data of SQL database.

### dataset/
stores the original dataset.

### load_sql_data.py
this file tranfer the original data into tables and chunks for our database use.


# Running the Program
Instructions on how to run the program:
```
$ python3 interface.py
Choose a database to use (SQL, noSQL) or exit:
```

# Interactive User Interface
```
Choose a database to use (SQL, noSQL) or exit:
```
Insert SQL or noSQL to choose which database to use.

or 

Insert 'exit' to terminate the program.

```
Choose a database to use (SQL, noSQL) or exit:SQL
MyDB> 
```
Now you can query from the database.

```
MyDB> GET name, email FROM Customer WHEN name LIKE %James
|name | email|
--------------
['Ashley James', 'marcus21@example.org']
['Clifford James', 'victoriawells@example.net']
['Catherine James', 'steven52@example.net']
['Chelsea James', 'dana94@example.net']
['Shane James', 'mhoward@example.com']
['Richard James', 'dennis52@example.org']
['Gregory James', 'hodgeschad@example.com']
['Rachel James', 'meghan71@example.net']
['Dawn James', 'sean39@example.net']
['Shelby James', 'colleencline@example.com']
['Timothy James', 'cathybarry@example.com']
['Blake James', 'gthompson@example.org']
['David James', 'nunezcarol@example.com']
['Mary James', 'fwallace@example.net']
['David James', 'skramer@example.com']
['Eric James', 'xmartin@example.net']
['Holly James', 'erogers@example.net']
['Linda James', 'jennifer40@example.org']
['Jacob James', 'jonathan60@example.org']
Run time: 0.03823590278625488 seconds.
```

# example queries
## SQL
Projection: GET name, email FROM Customer

Filtering:  GET name, email FROM Customer WHEN name LIKE %James

Join: GET ticket_id, p_name, rating FROM Ticket CONNECT Product ON Ticket.product_id = Product.product_id

Grouping: GET gender, name FROM Customer GATHER_BY Customer.gender WHEN name LIKE %James  

Aggregation: GET rating, CNT(rating) FROM Ticket GATHER_BY Ticket.rating

Ordering: GET gender, name, age FROM Customer GATHER_BY Customer.gender WHEN name LIKE %James ASCEND_BY age

Inserting: PUT 09900, Justin Chen, fdsfs@usc.edu, 23, Male IN Customer

Deleting: DROP FROM Customer WHEN name = 'Justin Chen'

Updating: CHANGE Customer WITH email = ggg@usc.edu, age = 43 WHEN name = 'Yolanda Miller'

