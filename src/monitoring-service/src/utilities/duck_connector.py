import duckdb
import os


class DuckConnector:

	def __init__(self):
		pass

	@staticmethod
	def create_db(database_name):
		con = duckdb.connect(f"{database_name}.db")
		return True

	@staticmethod
	def create_table(database_name, table_name, table_schema):
		con = duckdb.connect(f"{database_name}.db")
		con.execute(f"CREATE TABLE {table_name}{table_schema}")
		return True

	@staticmethod
	def delete_table(database_name, table_name):
		con = duckdb.connect(f"{database_name}.db")
		con.sql(f"DROP TABLE {table_name}")
		return True

	@staticmethod
	def execute_query(database_name, query):
		con = duckdb.connect(f"{database_name}.db")
		query_result = con.sql(query)
		return query_result

	@staticmethod
	def query_single_json(filename):
		return duckdb.execute(f"SELECT * FROM read_json_objects('{filename}', format='auto')").fetchone()[0]

	@staticmethod
	def query_partition(partition):
		file_list = os.listdir(partition)
		results = []
		for file in file_list:
			results.append(duckdb.execute(f"SELECT * FROM read_json_objects('{partition}/{file}', format='auto')").fetchone()[0])
		return results

"""
if __name__ == "__main__":
	print(DuckConnector.query_partition("2023-10-22"))
"""