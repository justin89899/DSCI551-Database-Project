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

# NoSQL
The NoSQL database revolves around a JSON structure, focusing on doctor's prescribing patterns.This is a database that predicts what characteristics and tendencies a doctor has in the drugs he prescribes based on his past experiences. The data is obtained by research in many ways. Each doctor in the database has its own unique national-provider-identifier, npi, by which we can find out exactly who each different doctor is. The type of medication prescribed by each doctor will be determined by a number of factors, such as: their location, direction of study and research, level of education, age, gender, and more. Therefore, this information is included in the provider_variables. And the data about the prescription drugs they prescribe is stored in cms_prescription_counts.

## GET:
GET provider_variables, npi, cms_prescription_counts FROM data WHEN {'provider_variables.specialty': "Nephrology"} GATHER_BY 'provider_variables.region' ASCEND_BY 'provider_variables.years_practicing

According to the characteristics of the data stored in this JSON file to carry out the data addition, deletion and modification operations. Because the data is obtained from multiple channels of investigation in order to ensure that the original data will not be easily altered, but from time to time new data will be added, so according to the original data as a time point, after which all the new data are stored in another new file. Therefore, if you want to insert new data into this database, the new data must meet the structure of the original data, i.e., it consists of three parts: npi, provider_variables, and cms_prescription_counts. In the use of PUT query，the inserting data format must also meet the JSON file format, and insert all the data at once.

## PUT:
PUT {"provider_variables": {"brand_name_rx_count": 150, "gender": "M", "generic_rx_count": 450, "region": "West", "settlement_type": "rural", "specialty": "General Practice", "years_practicing": 5}, "npi": "0987654321", "cms_prescription_counts": {"DrugA": 200, "DrugB": 100}} IN tables
we can check the results via GET:
GET provider_variables.specialty, provider_variables.region, npi FROM data WHEN {'npi': "0987654321"}

In the Change operation, in order to ensure the stability of the original data, the original data will not be a failure to modify a large number of changes, each time the modification must be specified under the conditions of the exact indicator under a certain indicator.

## CHANGE:
CHANGE provider_variables.region WITH North WHEN {'npi': "0987654321"}
we can check the results via GET:
GET provider_variables.region, npi FROM data WHEN {'npi': "0987654321"}

And in order to protect the integrity and security of the original data, it is not possible to directly delete all of a doctor's information in the process of deleting data. Each deletion step starts with a unit element, such as provider_variables.region, and each deletion must be conditioned to a specific value for a particular doctor or doctors. This protects the original dataset from losing most of the data due to deletion errors.

## DROP:
DROP provider_variables.region FROM table WHEN {'npi': "0987654321"}
we can check the results via GET:
GET provider_variables.region, npi FROM data WHEN {'npi': "0987654321"}
