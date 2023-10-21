import duckdb

my_schema = "(i INTEGER)"


class DuckConnector:

	def __init__(self):
		pass

	def create_db(database_name):
		con = duckdb.connect(f"{database_name}.db")
		return True

	def create_table(database_name, table_name, table_schema):
		con = duckdb.connect(f"{database_name}.db")
		con.execute(f"CREATE TABLE {table_name}{table_schema}")
		return True

	def delete_table(database_name, table_name):
		con = duckdb.connect(f"{database_name}.db")
		con.sql(f"DROP TABLE {table_name}")
		return True

	def execute_query(database_name, table_name, query):
		con = duckdb.connect(f"{database_name}.db")
		query_result = con.sql(query)
		return query_result
		