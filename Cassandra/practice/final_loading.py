import os
import json
import sys
from flask import Flask,request,render_template
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
import json
import logging
import datetime

#log = logging.getLogger()
#log.setLevel('DEBUG')
#handler = logging.StreamHandler()
#handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
#log.addHandler(handler)



### CONNECTING TO DB FUNCTION
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
def q_8():
	session = connect_to_db();

	create_table = "CREATE TABLE IF NOT EXISTS q_8(date text,tid text,tweet_text text,author_id text,location text,lang text,PRIMARY KEY(date,tid)) ;"
	
	session.execute(create_table)

	result  = "SELECT date,tid,tweet_text,author_id,location,lang FROM all_data;"
	rest = session.execute(result)
	
	
	
	for res in rest:
		
				user = {
							'date': res.date,
							'tid': res.tid,
							'tweet_text':res.tweet_text,
							'author_id':res.author_id,
							'lang':res.lang,
							'location' : res.location

				}
				# print(user)
				prepared = session.prepare('INSERT INTO q_8 JSON ?')
				session.execute(prepared,[json.dumps(user)])


def q_7():
	session = connect_to_db();

	create_table = "CREATE TABLE IF NOT EXISTS q_7 (hashtags text,date text,tid text,PRIMARY KEY(date,tid,hashtags)) ;"
	
	session.execute(create_table)

	result  = "SELECT hashtags,date,tid FROM all_data;"
	rest = session.execute(result)
	# prepared = session.prepare('INSERT INTO q_100 JSON ?')
	# session.execute(prepared,[json.dumps(rest)])
	for res in rest:
		
		the_list = res.hashtags
		if the_list and len(the_list)>0 and the_list.count('')!=len(the_list):
			
			for elements in the_list:

				user = {
							'hashtags': elements,
							'date': res.date,
							'tid': res.tid,
							

				}
				# print(user)
				prepared = session.prepare('INSERT INTO q_7 JSON ?')
				session.execute(prepared,[json.dumps(user)])
def q_6():
	session = connect_to_db();

	create_table = "CREATE TABLE IF NOT EXISTS q_6(location text,tid text,tweet_text text,author_id text,lang text,PRIMARY KEY(location,tid)) ;"
	
	session.execute(create_table)

	result  = "SELECT location,tid,tweet_text,author_id,lang FROM all_data;"
	rest = session.execute(result)
	
	
	
	for res in rest:
		if res.location :
				user = {
							'location': res.location,
							'tid': res.tid,
							'tweet_text':res.tweet_text,
							'author_id':res.author_id,
							'lang':res.lang

				}
				# print(user)
				prepared = session.prepare('INSERT INTO q_6 JSON ?')
				session.execute(prepared,[json.dumps(user)])

def q_5():
	session = connect_to_db();

	create_table = "CREATE TABLE IF NOT EXISTS q_5(date text,like_count int,tid text,tweet_text text,author_id text,location text,lang text,PRIMARY KEY(date,like_count,tid)) WITH CLUSTERING ORDER BY (like_count DESC);"
	
	session.execute(create_table)

	result  = "SELECT date,like_count,tid,tweet_text,author_id,location,lang FROM all_data;"
	rest = session.execute(result)
	
	for res in rest:
		user = {
					'date':res.date,
					'like_count': res.like_count,
					'tid': res.tid,
					'tweet_text':res.tweet_text,
					'author_id':res.author_id,
					'location':res.location,
					'lang':res.lang

		}
		# print(user)
		prepared = session.prepare('INSERT INTO q_5 JSON ?')
		session.execute(prepared,[json.dumps(user)])



def q_4():
	session = connect_to_db();

	create_table = "CREATE TABLE IF NOT EXISTS q_4(mentions text,datetime timestamp,tid text,tweet_text text,author_id text,location text,lang text,PRIMARY KEY(mentions,datetime,tid)) WITH CLUSTERING ORDER BY (datetime DESC);"
	
	session.execute(create_table)

	result  = "SELECT mentions,datetime,tid,tweet_text,author_id,location,lang FROM all_data;"
	rest = session.execute(result)
	# prepared = session.prepare('INSERT INTO q_100 JSON ?')
	# session.execute(prepared,[json.dumps(rest)])
	for res in rest:
		the_list = res.mentions

		if the_list and len(the_list)>0 and the_list.count('')!=len(the_list):
			
			for elements in the_list:

				user = {
							'mentions': elements,
							'datetime': str(res.datetime),
							'tid': res.tid,
							'tweet_text':res.tweet_text,
							'author_id':res.author_id,
							'location':res.location,
							'lang':res.lang

				}
				
				prepared = session.prepare('INSERT INTO q_4 JSON ?')
				session.execute(prepared,[json.dumps(user)])




def q_3():
	session = connect_to_db();

	create_table = "CREATE TABLE IF NOT EXISTS q_3(hashtags text,datetime timestamp,tid text,tweet_text text,author_id text,location text,lang text,PRIMARY KEY(hashtags,datetime,tid)) WITH CLUSTERING ORDER BY (datetime DESC);"
	
	session.execute(create_table)

	result  = "SELECT hashtags,datetime,tid,tweet_text,author_id,location,lang FROM all_data;"
	rest = session.execute(result)
	# prepared = session.prepare('INSERT INTO q_100 JSON ?')
	# session.execute(prepared,[json.dumps(rest)])
	for res in rest:
		the_list = res.hashtags

		if the_list and len(the_list)>0 and the_list.count('')!=len(the_list):
			
			for elements in the_list:

				user = {
							'hashtags': elements,
							'datetime': str(res.datetime),
							'tid': res.tid,
							'tweet_text':res.tweet_text,
							'author_id':res.author_id,
							'location':res.location,
							'lang':res.lang

				}
				
				prepared = session.prepare('INSERT INTO q_3 JSON ?')
				session.execute(prepared,[json.dumps(user)])

def q_2():
	session = connect_to_db();

	create_table = "CREATE TABLE IF NOT EXISTS q_2(keywords text,like_count int,tid text,tweet_text text,author_id text,location text,lang text,PRIMARY KEY(keywords,like_count,tid)) WITH CLUSTERING ORDER BY (like_count DESC);"
	
	session.execute(create_table)

	result  = "SELECT keywords_processed_list,like_count,tid,tweet_text,author_id,location,lang FROM all_data;"
	rest = session.execute(result)
	# prepared = session.prepare('INSERT INTO q_100 JSON ?')
	# session.execute(prepared,[json.dumps(rest)])
	for res in rest:
		the_list = res.keywords_processed_list

		if the_list and len(the_list)>0 and the_list.count('')!=len(the_list):
			
			for elements in the_list:

				user = {
							'keywords': elements,
							'like_count': res.like_count,
							'tid': res.tid,
							'tweet_text':res.tweet_text,
							'author_id':res.author_id,
							'location':res.location,
							'lang':res.lang

				}
				
				prepared = session.prepare('INSERT INTO q_2 JSON ?')
				session.execute(prepared,[json.dumps(user)])

def q_1():
	session = connect_to_db();

	create_table = "CREATE TABLE IF NOT EXISTS q_1(author_screen_name text,datetime timestamp,tid text,tweet_text text,author_id text,location text,lang text,PRIMARY KEY(author_screen_name,datetime,tid)) WITH CLUSTERING ORDER BY (datetime DESC);"
	
	session.execute(create_table)

	result  = "SELECT author_screen_name,datetime,tid,tweet_text,author_id,location,lang FROM all_data;"
	rest = session.execute(result)
	
	for res in rest:
		user = {
					'author_screen_name':res.author_screen_name,
					'datetime': str(res.datetime),
					'tid': res.tid,
					'tweet_text':res.tweet_text,
					'author_id':res.author_id,
					'location':res.location,
					'lang':res.lang

		}
		# print(user)
		prepared = session.prepare('INSERT INTO q_1 JSON ?')
		session.execute(prepared,[json.dumps(user)])







## This function process each file and inserts them into twitter dataset using dictionaries
def process(file):

	 #Load Whole File
	 with open(file) as dum:
	  tweetDateInfo = json.load(dum)

	 
	 session  =connect_to_db()

	 create_table = """CREATE TABLE IF NOT EXISTS all_data (
	      tid text PRIMARY KEY,
	      author text,
	      author_id text,
	      author_profile_image text,
	      author_screen_name text,
	      date text,
	      datetime timestamp,
	      hashtags list<text>,
	      keywords_processed_list list<text>,
	      lang text,
	      like_count int,
	      location text,
	      media_list map<text, frozen<map<text, text>>>,
	      mentions list<text>,
	      quote_count int,
	      quoted_source_id text,
	      reply_count int, 
	      replyto_source_id text,
	      retweet_count int,
	      retweet_source_id text,
	      sentiment int,
	      tweet_text text,
	      type text,
	      url_list list<text>,
	      verified text )  ;"""
	 
	 session.execute(create_table)
	 for (key,value) in tweetDateInfo.items()	:
	 	
	 	
	 	prepared = session.prepare('INSERT INTO all_data JSON ?')
	 	session.execute(prepared,[json.dumps(value)])



	 	

	 	
#This function creates the MAIN all_data table which contains every attribute and other tables use this main table to get the required info
def recurDir(filePath):
	
	for file in os.listdir(filePath):
						

						
						temp=filePath+'/'+file
						print(temp)
						process(temp)
						

	

def main():

	recurDir('workshop_dataset')
	#q_1()
	# q_4()
	# q_5()
	#q_8()











if __name__=='__main__':
	main()
