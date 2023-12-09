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
insert SQL or noSQL to choose which database to use.

or insert 'exit' to terminate the program.
