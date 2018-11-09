import os
import json
import sys
from flask import Flask,request,render_template
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
import datetime

app = Flask(__name__)



def connect_to_db():
	KEYSPACE = "twitter_dataset"

	cluster = Cluster(['127.0.0.1'])

	session = cluster.connect()
	session.execute("""
        CREATE KEYSPACE IF NOT EXISTS %s
        WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' }
        """ % KEYSPACE)
	session.set_keyspace(KEYSPACE)
	return session

#### This function does what is returns a list of  date of last 7 days as strings in an array so that we can iterate over it to select the fields we want to in a range of dates
# def date_formatting(value):
# 	x = value.split('-')
# 	year = int(x[0])
# 	month = int(x[1])
# 	date = int(x[2])
# 	mydate = datetime.date(year,month,date)
# 	date_list = []
# 	date_list.append(value)
# 	for i in range(1,7):
# 			mydate -= datetime.timedelta(days=1)
# 			date_list.append(str(mydate))
# 	return date_list


### This function ensures that whatever format date we send like 2018-2-9 but this type of date is not their in my json instead i convert this to 2018-02-09 which is present in my database
# def date_format(value):
# 	x = value.split('-')
# 	year = int(x[0])
# 	month = int(x[1])
# 	date = int(x[2])
# 	mydate = datetime.date(year,month,date)
# 	return mydate	


# This function adds a ' ' the apostrophe for text attribrutes	
def processString(value):

		processed = "\'%s\'" %value
		return processed



def query_20_sub(value):
	session = connect_to_db()
	

	result  = "SELECT * FROM counter_pair_hash ;"

	rest = session.execute(result)
	dummy_date = value #### THIS WILL BE THE INPUT IN FLASK
	for res in rest:
		
			user = {
						'pair': res.pair,
						'freq': int(res.counter_value),
						'date' : dummy_date
						

			}
			
			prepared = session.prepare('INSERT INTO counter_pair_real JSON ?')
			session.execute(prepared,[json.dumps(user)])
@app.route('/query_20',methods = ['POST','GET'])
def query_20():

	session = connect_to_db()

	create_table = "CREATE TABLE IF NOT EXISTS counter_pair_hash(counter_value counter,pair text,PRIMARY KEY(pair)) ;"
	
	session.execute(create_table)

	create_table_real = "CREATE TABLE IF NOT EXISTS counter_pair_real(date text,freq int,pair text,PRIMARY KEY(date,freq,pair)) WITH CLUSTERING ORDER BY (freq DESC) ;"

	session.execute(create_table_real)

	x = request.form['date']
	# x = date_format(x)

	truncating = "TRUNCATE TABLE counter_pair_real ;"
	session.execute(truncating)
	truncate_hash = "TRUNCATE TABLE counter_pair_hash ;"
	session.execute(truncate_hash)

	# date_list = date_formatting(x)

	
	result  = "SELECT pair FROM q_11 where date = " + processString(x) + " ;"
	rest = session.execute(result)
	# return render_template('index.html',output = rest,nu=10)
	for r in rest:
						
			combine= r.pair
			session.execute("UPDATE counter_pair_hash  SET counter_value = counter_value+ 1  WHERE pair=" + processString(combine) + " ;" )

	session.shutdown()

	query_20_sub(x)


	session = connect_to_db()


	exe = "SELECT * FROM counter_pair_real  ;"
	result = session.execute(exe)
	exe = "SELECT COUNT(*) FROM counter_pair_real  ;"
	total_rows = session.execute(exe)
	answer = 0
	for r in total_rows:
		answer = r.count
	answer = str(answer)
	return render_template('index.html',output = result,nu = 20,row_total = answer)
def query_10_sub():
	session = connect_to_db()
	

	result  = "SELECT * FROM counter_location_hash ;"

	rest = session.execute(result)
	dummy_date = 'dummy' #### THIS WILL BE THE INPUT IN FLASK
	for res in rest:
		if str(res.location)!='None':
			user = {
						'location': res.location,
						'freq': int(res.counter_value),
						'dummy' : dummy_date
						

			}
			
			prepared = session.prepare('INSERT INTO counter_location_real JSON ?')
			session.execute(prepared,[json.dumps(user)])
@app.route('/query_10',methods = ['POST','GET'])
def query_10():

	session = connect_to_db()

	create_table = "CREATE TABLE IF NOT EXISTS counter_location_hash(counter_value counter,location text,PRIMARY KEY(location)) ;"
	
	session.execute(create_table)

	create_table_real = "CREATE TABLE IF NOT EXISTS counter_location_real(dummy text,freq int,location text,PRIMARY KEY(dummy,freq,location)) WITH CLUSTERING ORDER BY (freq DESC) ;"

	session.execute(create_table_real)

	x = request.form['locations']

	truncating = "TRUNCATE TABLE counter_location_real ;"
	session.execute(truncating)
	truncate_hash = "TRUNCATE TABLE counter_location_hash ;"
	session.execute(truncate_hash)

	# date_list = date_formatting(x)

	
	result  = "SELECT location FROM q_3 where hashtags = " + processString(x) + " ;"
	rest = session.execute(result)
	# return render_template('index.html',output = rest,nu=10)
	for r in rest:
						
			loc= r.location
			session.execute("UPDATE counter_location_hash  SET counter_value = counter_value+ 1  WHERE location=" + processString(loc) + " ;" )

	session.shutdown()

	query_10_sub()


	session = connect_to_db()


	exe = "SELECT * FROM counter_location_real  ;"
	result = session.execute(exe)
	exe = "SELECT COUNT(*) FROM counter_location_real  ;"
	total_rows = session.execute(exe)
	answer = 0
	for r in total_rows:
		answer = r.count
	answer = str(answer)
	return render_template('index.html',output = result,nu = 10,row_total = answer)



@app.route('/')
def main_page():
	return render_template('index.html')


if __name__=='__main__':
	app.run(debug=True)
	# main()