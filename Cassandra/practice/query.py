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
def date_formatting(value):
	x = value.split('-')
	year = int(x[0])
	month = int(x[1])
	date = int(x[2])
	mydate = datetime.date(year,month,date)
	date_list = []
	date_list.append(value)
	for i in range(1,7):
			mydate -= datetime.timedelta(days=1)
			date_list.append(str(mydate))
	return date_list


### This function ensures that whatever format date we send like 2018-2-9 but this type of date is not their in my json instead i convert this to 2018-02-09 which is present in my database
def date_format(value):
	x = value.split('-')
	year = int(x[0])
	month = int(x[1])
	date = int(x[2])
	mydate = datetime.date(year,month,date)
	return mydate	


# This function adds a ' ' the apostrophe for text attribrutes	
def processString(value):

		processed = "\'%s\'" %value
		return processed


@app.route('/query_8',methods = ['POST','GET'])
def query_8():

	

	

	x  = request.form['delete']

	x = date_format(x)

	session = connect_to_db()

	exe = "SELECT * FROM q_8  where date = " + processString(x) + " ;"
	result = session.execute(exe)

	return render_template('index.html',output = result,nu = 8)





@app.route('/query_1',methods = ['POST','GET'])
def query_1():

	

	

	x  = request.form['author_screen_name']

	

	session = connect_to_db()

	exe = "SELECT * FROM q_1  where author_screen_name = " + processString(x) + " ;"
	result = session.execute(exe)

	return render_template('index.html',output = result,nu = 1)

@app.route('/query_2',methods = ['POST','GET'])
def query_2():

	

	

	x  = request.form['keywords']

	

	session = connect_to_db()

	exe = "SELECT * FROM q_2  where keywords = " + processString(x) + " ;"
	result = session.execute(exe)

	return render_template('index.html',output = result,nu = 2)


@app.route('/query_3',methods = ['POST','GET'])
def query_3():

	

	

	x  = request.form['hashtags']

	

	session = connect_to_db()

	exe = "SELECT * FROM q_3  where hashtags = " + processString(x) + " ;"
	result = session.execute(exe)

	return render_template('index.html',output = result,nu = 3)


@app.route('/query_4',methods = ['POST','GET'])
def query_4():

	

	

	x  = request.form['author_screen_name']

	

	session = connect_to_db()

	exe = "SELECT * FROM q_4  where mentions = " + processString(x) + " ;"
	result = session.execute(exe)

	return render_template('index.html',output = result,nu = 4)





@app.route('/query_5',methods = ['POST','GET'])
def query_5():

	

	x  = request.form['date']

	x = date_format(x)

	session = connect_to_db()

	exe = "SELECT * FROM q_5  where date = " + processString(x) + " ;"
	result = session.execute(exe)

	return render_template('index.html',output = result,nu = 5)







@app.route('/query_6',methods = ['POST','GET'])
def query_6():

	session = connect_to_db()

	

	x  = request.form['location']



	exe = "SELECT * FROM q_6  where location = " + processString(x) + " ;"
	result = session.execute(exe)

	return render_template('index.html',output = result,nu = 6)

def query_7_sub():
	session = connect_to_db()
	

	result  = "SELECT * FROM counter_hash ;"

	rest = session.execute(result)
	dummy_date = 'dummy' #### THIS WILL BE THE INPUT IN FLASK
	for res in rest:
		user = {
					'hashtags': res.hashtags,
					'freq': int(res.counter_value),
					'date' : dummy_date
					

		}
		
		prepared = session.prepare('INSERT INTO counter_real JSON ?')
		session.execute(prepared,[json.dumps(user)])






@app.route('/query_7',methods = ['POST','GET'])
def query_7():

	session = connect_to_db()

	create_table = "CREATE TABLE IF NOT EXISTS counter_hash(counter_value counter,hashtags text,PRIMARY KEY(hashtags)) ;"
	
	session.execute(create_table)

	create_table_real = "CREATE TABLE IF NOT EXISTS counter_real(date text,freq int,hashtags text,PRIMARY KEY(date,freq,hashtags)) WITH CLUSTERING ORDER BY (freq DESC) ;"

	session.execute(create_table_real)

	x = request.form['date']

	truncating = "TRUNCATE TABLE counter_real ;"
	session.execute(truncating)
	truncate_hash = "TRUNCATE TABLE counter_hash ;"
	session.execute(truncate_hash)

	date_list = date_formatting(x)

	for last_days in date_list:
			result  = "SELECT hashtags FROM q_7 where date = " + processString(last_days) + " ;"
			rest = session.execute(result)
			for r in rest:
						
						hash_bro = r.hashtags  
						session.execute("UPDATE counter_hash  SET counter_value = counter_value+ 1  WHERE hashtags=" + processString(hash_bro) + " ;" )

	session.shutdown()

	query_7_sub()


	session = connect_to_db()


	exe = "SELECT * FROM counter_real  LIMIT 20 ;"
	result = session.execute(exe)
		
	return render_template('index.html',output = result,nu = 7)
	
@app.route('/')
def main_page():
	return render_template('index.html')



if __name__=='__main__':
	app.run(debug=True)
	main()