import os

# Assuming you cloned the repository to a directory called 'repository'
repo_dir = '/Workspace/Repos/girish.sharma@databricks.com/girishrepo'
sql_file_path = os.path.join(repo_dir, '/Workspace/Repos/girish.sharma@databricks.com/girishrepo/mytestsql.sql')

# Read the SQL file
with open(sql_file_path, 'r') as file:
    sql_query = file.read()

# Now you can use sql_query to execute it in your database
print(sql_query)
