# W4111_F19_HW1
Implementation template for homework 

### CSVDataTable
1. The functions critically depend on two functions: find_by_template and find_by_primary_key
2. find_by_primary_key is very similar to the design of find_by_template; just checking whether the values match in the row correctly. 
3. For all the methods based on keys, we just find the particular element by the key and then perform the operation. 
4. There's an added method to check if the primary keys getting passed into the table aren't invalid (like None or "")
5. Update has an extra to check if the primary key itself isn't being updated to something that conflicts.
6. For all the methods based on templates, they find the respective elements using the template method and perform the required operations. 
7. Update again has an extra check to see if the updated values contain primary keys and they do not create conflicts or invalid values. 
8. Insert has similar checks to updates to check if correct and valid values are bring entered into the table. 
9. Save function implements the save functionality.  

### RDBDataTable
1. The heart of the class is the function _run_sql_query, which gets a sql query in the form of a string and processes it. 
2. The other methods just create the SQL queries in text based on the fields they get and pass it along to the above function. 

### Tests
1. The test_CSVDataTable.py and test_RDBDataTable.py use unittests to assert the different scenarios easily.
2. The test functions have the appropriate documentation for description of scenarios. 
3. The revenue test file outputs the appropriate data for the selected query.  