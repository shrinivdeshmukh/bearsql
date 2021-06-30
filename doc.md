# bearsql
Bearsql allows you to query pandas dataframe with sql syntax. It uses [duckdb](https://duckdb.org/) as the internal processing engine.

## Usage

**To use bearsql in a project:**
```
from bearsql import SqlContext
import pandas as pd

sc = SqlContext()
# The above statement will create duckdb instance in memory. Once the session ends, the database will be erased and not be persisted
# To persist the database, you can instantiate sqlcontext like:
# sc = SqlContext(database='<YOUR_DATABASE_NAME>.db'

df = pd.DataFrame([{'name': 'John Doe', 'city': 'New York', 'age': 24}, {'name': 'Jane Doe', 'city': 'Chicago', 'age': 27}])

# Create table from pandas dataframe
sc.register_table(df, 'testable') # <YOUR_TABLENAME> instead of 'testable'

# Query table and output to pandas dataframe
results = sc.sql('select * from testable', output='df')
output_df = next(results)
print(output_df)

# Query table and output to pyarrow table
results = sc.sql('select * from testable', output='arrow')
output_arrow_table = next(results)
print(output_arrow_table)

# Query table and output raw tuples
results = sc.sql('select * from testable', output='any')
output_rows = next(results)
print(output_rows)
```

**Create a relational table from dataframe and apply some operations:**
```
rel = sc.relation(df, 'new_relation') # <YOUR_RELATION_NAME> instead of new_relation

print(rel.filter('age > 24'))

# OR convert to df:

rel.filter('age > 24').df()
```

**Export the data to filesystem:**
```
result = sc.sql('EXPORT DATABASE \'<OUTPUT_FOLDER>\' (FORMAT PARQUET);') # format can either be PARQUET or CSV
list(result)
```
For more examples, please visit https://github.com/duckdb/duckdb/blob/master/examples/python/duckdb-python.py 
