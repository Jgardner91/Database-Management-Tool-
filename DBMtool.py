import pyodbc
import pandas as pd 
import sqlalchemy as db
from sqlalchemy import create_engine
from sqlalchemy import join
from sqlalchemy.sql import select
import warnings
warnings.filterwarnings('ignore')
import urllib.parse
from sqlalchemy import func

from sqlalchemy import event
from sqlalchemy.engine import Engine
import time
import logging
import datetime 

from sqlalchemy import Table, Column, Integer, String, Numeric,Float, MetaData,distinct,delete
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Numeric
from sqlalchemy.orm import sessionmaker

import hashlib as h
from sqlalchemy.sql import alias

from sqlalchemy import func
#importing seaborn library for plotting results
import seaborn as sns



# getting connection variables for SQL Server
def SQLServerConnector(database):

	# can eliminate if statment by passing necessary parameters 
	# through connection string

	if database == 'database':
		parameters = urllib.parse.quote("DRIVER={ODBC Driver 17 for SQL Server};"
									"SERVER=InputServerName;"
									
									"DATABASE=InputDatabaseName;"
									"Trusted_Connection=yes/no")
	
		engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(parameters),fast_executemany = True)
		connection = engine.connect()
		metadata = db.MetaData()
		raw_connect = engine.raw_connection()

	elif database == 'database':
		parameters = urllib.parse.quote("DRIVER={ODBC Driver 17 for SQL Server};"
									"SERVER=CHACXSQL1\ACXIOM;"
									
									"DATABASE=TrueGreen;"
									"Trusted_Connection=yes")
	
		engine = create_engine("mssql+pyodbc:///?odbc_connect={}".format(parameters),fast_executemany = True)
		connection = engine.connect()
		metadata = db.MetaData()
		raw_connect = engine.raw_connection()

	


	else:
		print("Database Does not Exist. Please Enter a valid database name.")

	return engine,connection,metadata,raw_connect

def SQLiteConnector(database_name):


# getting connection variables for SQLlite 
	aux_engine = create_engine('sqlite:///{}.db'.format(database_name),echo =True)
	aux_connection = aux_engine.connect()
	aux_metadata = db.MetaData()
	return aux_engine,aux_connection,aux_metadata


class Connection:
	'''
	This class defines the connection parameters.
	Instance variables to be adopted by Database Classes
	'''
	def __init__(self,engine,connection,metadata,raw_connect):
		self.engine = engine
		self.connection = connection
		self.metadata = metadata
		self.raw_connect = raw_connect


class SQLite(Connection):
	def __init__(self,engine,connection,metatdata):
		super().__init__(aux_engine,aux_connection,aux_metadata)


	def CreateTable(self,table_name):
		table = Table( 
			table_name, self.metadata,
			Column('id',Integer,primary_key=True))
		self.metadata.create_all(self.engine)

	def LoadTable(self,table_name,file):
		pass



class DatabaseName(Connection):
	
	def __init__(self,engine,connection,metadata,raw_connect):
		super().__init__(engine,connection,metadata,raw_connect)
		
		# accesing table names
		# making a list of table name objects to use in queries
		# maping columns from the table list
		self.table_list = self.engine.table_names()
		self.table_objs = [db.Table(tbl,self.metadata,autoload=True,autoload_with = self.connection) for tbl in self.table_list]
		self.column_map = {self.table_list[i]:[c.name for c in self.table_objs[i].columns] for i in range(len(self.table_list))}
		self.table_map =  {}


	def RefTable(self):

		cmap = self.column_map
		tobjs = self.table_objs
		table_dict = {}
		for i,key in enumerate(cmap.keys()):
			table_dict[key] = tobjs[i]

		return table_dict 

	def SelectInstall(self):
		"""
		Method: This method allows you to select which acxiom install month and year
				you would like to query. 
		Parameters: None
		Returns: name of table for query

		Notes: Enter an integer like '9' for the month and an integer like '2021'
				for the year.
		"""
		table_type = int(input("Enter 1 for an Acxiom Table and 0 for Other: "))
		if table_type == 1:
			KEY = input("Enter 'CURRENT' for current month install, Enter 'PREVIOUS' for previous month install: ").upper()
			if KEY == 'CURRENT':
				KEY = 'CurrentInstall'
			if KEY == 'PREVIOUS':
				KEY = 'PreviousInstall'
			
			return KEY,table_type
		
		if table_type == 0:
			table_name = str(input("Enter Table Name: "))
			table_map = {'AddressTable':'AddressTable'}
			table_map[table_name] = table_name
			return table_map[table_name],table_type

	def SelectAll(self,ofset,amount):
		"""
		Method: This method selects all fields and all rows or up to x number of rows.
		Parameter: Takes integer limiting the number of rows returned 
		Returns: DataFrame

		Notes: Good to use if you have no field restrictions, i.e. you'd like to selct every field 
				but maybe not every row. Cannot customize field selection.
		"""
		
		install,table_type = self.SelectInstall()


		if table_type == 1:

			table = self.RefTable()[install]
			query = db.select([table]).order_by(table.columns['State']).offset(ofset).limit(amount)
			ResultProxy = self.connection.execute(query)
			ResultSet = ResultProxy.fetchall()
			Pull_ = pd.DataFrame(ResultSet, columns = self.column_map[install])
			return Pull_ 

		if table_type == 0:
			table = self.RefTable()[install]
			query = db.select([table]).order_by(table.columns['IndvKey']).offset(ofset).limit(amount)
			ResultProxy = self.connection.execute(query)
			ResultSet = ResultProxy.fetchall()
			Pull_ = pd.DataFrame(ResultSet, columns = self.column_map[install])
			return Pull_ 




	def ChoseFields(self,install):

		"""
		Method: This method allows you to chose which fields you'd like to query.
		Parameter: install. read in from different method
		Returns: DataFrame

		Notes: The way you chose your fields is by a mapping that is printed out when you 
				run this method. The mapping takes in an integer and reutrns the associated 
				field. You just need to look through the list to make sure your selecting 
				the right integer for the field you want. 
		"""
		fields = self.column_map[install]
		mapping = {i:fields[i] for i in range(len(fields))}
		print(mapping)
		num_fields = int(input("How many fields would you like to chose(think about it): "))
		X = [mapping[int(input("Chose Field(integer): "))] for i in range(num_fields) ]
		return X

	def CustomPull(self):

		"""
		Method: This method does the actual quering for the custom selection of fields.
		Parameters: Takes an integer limiting the number of rows returned
		Returns: DataFrame

		Notes: Make sure you take your time when inputing which fields you'd like. 
		"""
		install = self.SelectInstall()[0]
		table = self.RefTable()[install]
		X = self.ChoseFields(install)
		num_states = str(input("How many states would you like to include in the Query: "))
		
		if num_states != 'all':
			states = [str(input("Select State: ")).upper() for i in range(int(num_states))]
			base_query = db.select([table.columns[column] for column in X]).where(table.columns["State"].in_(states))
			query = base_query.order_by(table.columns['State'])
			ResultProxy = self.connection.execute(query)
			ResultSet = ResultProxy.fetchall()
			Pull_ = pd.DataFrame(ResultSet, columns = X  )
			return Pull_
		
		if num_states == 'all':
			print('inside conditional 2')
			query = db.select([table.columns[column] for column in X])
			ResultProxy = self.connection.execute(query)
			ResultSet = ResultProxy.fetchall()
			Pull_ = pd.DataFrame(ResultSet, columns = X  )
			return Pull_

	def OnePercent(self):
		install = self.SelectInstall()[0]
		raw_sql = "EXEC ProcedureName"
		connection = self.engine.raw_connection()
		cursor = connection.cursor()
		cursor.execute(raw_sql)
		results = cursor.fetchall()
		ResultSet = {i:list(row) for i,row in enumerate(results)}
		Pull_ = pd.DataFrame(ResultSet).T
		Pull_.rename(columns = {i:ColumnName for i,ColumnName in enumerate(self.column_map[install])},inplace=True)
		Pull_.to_pickle("OnePercentSample.pkl")	
		cursor.close()
		return Pull_
		
	def SelectState(self):

		"""
		Method: This method selects all fields based but includes only user chosen number of states
		Parameters: Takes an integer limiting the number of rows returned 
		Returns: DataFrame

		Notes: Make sure to take your time selecting the number of states. Uppercase  or lowercase , or combocase
				is allowed.
		"""
		install = self.SelectInstall()[0]
		table = self.RefTable()[install]
		num_states = int(input("How many states would you like to include in the Query: "))
		states = [str(input("Select State: ")).upper() for i in range(num_states)]
		base_query = db.select(['*'])
		query = base_query.where(table.columns["State"].in_(states))
		start_time = datetime.datetime.now()
		ResultProxy = self.connection.execute(query)
		ResultSet = ResultProxy.fetchall()
		Pull_ = pd.DataFrame(ResultSet, columns = self.column_map[install] )
		end_time = datetime.datetime.now()
		delta = end_time - start_time
		print(delta)
		return Pull_ 