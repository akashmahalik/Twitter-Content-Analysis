from py2neo import Graph, Path,authenticate
import json
import sys
import os

def connect_to_db():
	authenticate("localhost:7474", "neo4j", "connecttoneo4j")
	graph = Graph()
	return graph
	


textAttrib = ["author","author_id","datetime","date","verified","retweet_source_id","location","type","quoted_source_id","tweet_text","author_profile_image","author_screen_name","replyto_source_id","lang"]

intAttrib = ["quote_count","reply_count","sentiment","retweet_count","like_count"]
listAttrib = ["url_list","keywords_processed_list","mentions","hashtags"]


		


def query_5_reply(listTweet):
	graph = connect_to_db()
	final_query = """

	UNWIND {json} as tweet


		


		
		
		

		
		WITH tweet
		UNWIND tweet.replyto_source_id AS reply
		WITH reply ,tweet
		MATCH(tid:TID{tid:reply	})
		WITH collect(tid) AS l_tid,reply,tweet
		UNWIND l_tid AS id_x
		MATCH(id_x)-[:AUTHOR_ID]->(parent_author)
		MATCH(childReply:Author_id{author_id:tweet.author_id})
		
		CREATE (parent_author)<-[:REPLYTO_SOURCE_ID]-(childReply)






		
			




		



			
			



	




	"""	

	graph.run(final_query,json=listTweet)
def query_5_retweet(listTweet):
	graph = connect_to_db()
	final_query = """

	UNWIND {json} as tweet


		


		
		
		

		
		WITH tweet
		UNWIND tweet.retweet_source_id AS retweet 
		WITH retweet ,tweet
		MATCH(tid:TID{tid:retweet	})
		WITH collect(tid) AS l_tid,retweet,tweet
		UNWIND l_tid AS id_x
		MATCH(id_x)-[:AUTHOR_ID]->(parent_author)
		MATCH(childRetweet:Author_id{author_id:tweet.author_id})
		
		CREATE (parent_author)<-[:RETWEET_SOURCE_ID]-(childRetweet)






		
			




		



			
			



	




	"""

	graph.run(final_query,json = listTweet)





def query_1(listTweet):
	graph = connect_to_db()
	final_query = """

	foreach (tweet in {json}|

		MERGE(tid:TID{tid:tweet.tid,
		like_count : tweet.like_count,
		quote_count : tweet.quote_count,
		sentiment : tweet.sentiment,
		quote_count : tweet.quote_count,
		retweet_count : tweet.retweet_count,
		tweet_text : tweet.tweet_text
		})
		

		foreach (keyword in tweet.keywords_processed_list|

		MERGE(childKey:Keyword
				{

				keyword:keyword


				}


				)
		MERGE (tid)-[:KEYWORD]->(childKey)		



		)


		foreach (hashtag in tweet.hashtags|

		MERGE(childHash:Hashtag
				{

				hashtag:hashtag


				}


				)
		MERGE (tid)-[:HASHTAG]->(childHash)		



		)
		foreach (mention in tweet.mentions|

		MERGE(childMention:Mention
				{

				mention:mention


				}


				)
		MERGE (tid)-[:MENTION]->(childMention)	




		)

		foreach (types in tweet.type|

		MERGE(childType:Type
				{

				type:types


				}


				)
		MERGE (tid)-[:TYPE]->(childType)	




		)

			foreach (loc in tweet.location|

		MERGE(childLocation:Location
				{

				location:loc


				}


				)
		MERGE (tid)-[:LOCATION]->(childLocation)	




		)
			


			foreach (date in tweet.date|

		MERGE(childDate:Date__
				{

				date:date
				

				}


				)
		MERGE (tid)-[:DATE]->(childDate)	




		)
			foreach (ver in tweet.verified|

		MERGE(childVer:Verified
				{

				verified:ver


				}


				)
		MERGE (tid)-[:VERIFIED]->(childVer)	




		)

			foreach (screen_name in tweet.author_screen_name|

		MERGE(childScreen:ScreenName
				{

				author_screen_name:screen_name


				}


				)
		MERGE (tid)-[:AUTHOR_SCREEN_NAME]->(childScreen)	




		)

			foreach (lang in tweet.lang|

		MERGE(childLang:Lang
				{

				lang:lang


				}


				)
		MERGE (tid)-[:LANGUAGE]->(childLang)	




		)

		foreach (auth in tweet.author|




		MERGE(childAuthorId:Author_id
				{

				author_id :tweet.author_id


				}


				)

		MERGE (tid)-[:AUTHOR_ID]->(childAuthorId)	
		MERGE(childAuthorName:Author_name
				{

				author_name : auth


				}


				)
		MERGE (childAuthorId)-[:AUTHOR_NAME]->(childAuthorName)	
		



		)





	)











	"""
	
	graph.run(final_query,json = listTweet)

def process(file):

	with open(file) as dum:
		tweetDateInfo = json.load(dum)

	listTweet = []
	for (key,value) in tweetDateInfo.items():
			listTweet.append(value)
	
	query_1(listTweet)
	# query_1(listTweet)
	# query_1(listTweet)
	# for x in listTweet:
	# 	query_1(x)
			# create_node(value)
def process_reply_retweet(file):
	with open(file) as dum:
		tweetDateInfo = json.load(dum)

	listTweet = []
	for (key,value) in tweetDateInfo.items():
			listTweet.append(value)
	
	query_5_retweet(listTweet)
	query_5_reply(listTweet)

def recurDir(filePath):
	query = """ CREATE CONSTRAINT ON (a:TID) ASSERT (a.tid) IS UNIQUE
				CREATE CONSTRAINT ON (b:Hashtag) ASSERT (b.hashtag) IS UNIQUE
				CREATE CONSTRAINT ON (c:Keyword) ASSERT (c.keyword) IS UNIQUE
				CREATE CONSTRAINT ON (d:Mention) ASSERT (d.mention) IS UNIQUE
				CREATE CONSTRAINT ON (e:Type) ASSERT (e.type) IS UNIQUE
				CREATE CONSTRAINT ON (f:Location) ASSERT (f.location) IS UNIQUE
				CREATE CONSTRAINT ON (g:Date__) ASSERT (g.date) IS UNIQUE
				CREATE CONSTRAINT ON (h:Verified) ASSERT (h.verified) IS UNIQUE
				CREATE CONSTRAINT ON (i:ScreenName) ASSERT (i.author_screen_name) IS UNIQUE
				CREATE CONSTRAINT ON (j:Lang) ASSERT (j.lang) IS UNIQUE
				CREATE CONSTRAINT ON (k:Author_name) ASSERT (k.author_name) IS UNIQUE
				CREATE CONSTRAINT ON (l:Author_id) ASSERT (l.author_id) IS UNIQUE

	"""
	graph = connect_to_db()

	a = "CREATE CONSTRAINT ON (a:TID) ASSERT (a.tid) IS UNIQUE"
	graph.run(a)
	b = "CREATE CONSTRAINT ON (b:Hashtag) ASSERT (b.hashtag) IS UNIQUE"
	graph.run(b)
	c = "CREATE CONSTRAINT ON (c:Keyword) ASSERT (c.keyword) IS UNIQUE"
	graph.run(c)
	d = "CREATE CONSTRAINT ON (d:Mention) ASSERT (d.mention) IS UNIQUE"
	graph.run(d)
	e = "CREATE CONSTRAINT ON (e:Type) ASSERT (e.type) IS UNIQUE"
	graph.run(e)
	f = "CREATE CONSTRAINT ON (f:Location) ASSERT (f.location) IS UNIQUE"
	graph.run(f)
	g = "CREATE CONSTRAINT ON (g:Date__) ASSERT (g.date) IS UNIQUE"
	graph.run(g)
	h = "CREATE CONSTRAINT ON (h:Verified) ASSERT (h.verified) IS UNIQUE"
	graph.run(h)
	i = "CREATE CONSTRAINT ON (i:ScreenName) ASSERT (i.author_screen_name) IS UNIQUE"
	graph.run(i)
	j = "CREATE CONSTRAINT ON (j:Lang) ASSERT (j.lang) IS UNIQUE"
	graph.run(j)
	k = "CREATE CONSTRAINT ON (k:Author_name) ASSERT (k.author_name) IS UNIQUE"
	graph.run(k)
	l = "CREATE CONSTRAINT ON (l:Author_id) ASSERT (l.author_id) IS UNIQUE"
	graph.run(l)
	
	
	# for file in os.listdir(filePath):
						

						
	# 					temp=filePath+'/'+file
	# 					print(temp)
	# 					process(temp)
	for file in os.listdir(filePath):
						

						
						temp=filePath+'/'+file
						print(temp)
						process_reply_retweet(temp)					
recurDir('workshop_dataset')
