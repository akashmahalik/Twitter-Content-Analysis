import os
import json
import sys
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
import json
import logging
import datetime



log = logging.getLogger()
log.setLevel('DEBUG')
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)



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

def q_13():
	session = connect_to_db();

	create_table = "CREATE TABLE IF NOT EXISTS q_13(date text,tid text,mentions list<text>,hashtags list<text>,PRIMARY KEY(date,tid)) ;"
	
	session.execute(create_table)

	result  = "SELECT date,tid,mentions,hashtags FROM all_data;"
	rest = session.execute(result)
	
	
	
	for res in rest:
		hash_list = res.hashtags
		ment_list = res.mentions
		if hash_list and len(hash_list)>0 and hash_list.count('')!=len(hash_list) and ment_list and len(ment_list)>0 and ment_list.count('')!=len(ment_list):
							user = {
										'date': res.date,
										'tid': res.tid,
										'mentions' : res.mentions,
										'hashtags' : res.hashtags

							}
							
							prepared = session.prepare('INSERT INTO q_13 JSON ?')
							session.execute(prepared,[json.dumps(user)])


def q_11():
	session = connect_to_db();

	create_table = "CREATE TABLE IF NOT EXISTS q_11 (pair text,date text,tid text,PRIMARY KEY(date,tid,pair)) ;"
	
	session.execute(create_table)

	result  = "SELECT hashtags,date,tid,mentions FROM q_13;"
	rest = session.execute(result)
	# prepared = session.prepare('INSERT INTO q_100 JSON ?')
	# session.execute(prepared,[json.dumps(rest)])
	for res in rest:
		
		the_list = res.hashtags
		for elements in the_list:
					inner_list = res.mentions
					for inner_element in inner_list:

											user = {
														'pair': elements + '-' + inner_element,
														'date': res.date,
														'tid': res.tid,
														

											}
											# print(user)
											prepared = session.prepare('INSERT INTO q_11 JSON ?')
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
						
						process(temp)
						

	
def main():
	recurDir('workshop_dataset')
	q_3()
	q_11()
	q_13()





if __name__=='__main__':
	main()


