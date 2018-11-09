from py2neo import Graph, Path,authenticate
import json
import sys
import os
import json
def connect_to_db():
	authenticate("localhost:7474", "neo4j", "connecttoneo4j")
	graph = Graph()
	return graph
	


textAttrib = ["author","author_id","datetime","date","verified","retweet_source_id","location","type","quoted_source_id","tweet_text","author_profile_image","author_screen_name","replyto_source_id","lang"]

intAttrib = ["quote_count","reply_count","sentiment","retweet_count","like_count"]
listAttrib = ["url_list","keywords_processed_list","mentions","hashtags"]


		








def query_1(listTweet):
	graph = connect_to_db()
	final_query = """

	foreach (tweet in {json}|

		MERGE(tid:TID{tid:tweet.tid,
		
		type : tweet.type
		})
		



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


		
			


			
			
			foreach (screen_name in tweet.author_screen_name|

		MERGE(childScreen:ScreenName
				{

				author_screen_name:screen_name


				}


				)
		MERGE (tid)-[:AUTHOR_SCREEN_NAME]->(childScreen)	




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

		foreach (reply in tweet.replyto_source_id|

		MERGE(childReply:Replyto
				{

				replyto_source_id:reply


				}


				)
		MERGE (tid)-[:REPLYTO_SOURCE_ID]->(childReply)	




		)






	)











	"""
	
	graph.run(final_query,json = listTweet)



def update_without_overwriting(d, x):
    dict.update({k: v for k, v in x.items() if k not in d})

def q_7():
	query = """
	MATCH (x)<-[:HASHTAG]-(z:TID{type:'Tweet'})-[:MENTION]->(y) WHERE x.hashtag = {hash}
	 WITH count(y.mention) AS count,x as hashtag,y as mention,collect(z) AS TIDs 
	  RETURN hashtag,TIDs,mention,count
	   order by count desc
	    LIMIT 3


	 """
	graph = connect_to_db(	)
	result = graph.run(query).data()
	print(result)
	
	with open('7.json', 'w') as fp:
    	json.dump(result, fp)
	# q  = {}
	# for i in result:
	# 	# print(i)
	# 	print('\n\n\n\n')
	# 	update_without_overwriting(q,i)
	# 	print(q)
	# print(q)	


def q_11():
	query = """
	MATCH (x)<-[:AUTHOR_SCREEN_NAME]-(z_1:TID{type:'Tweet'}) WHERE x.author_screen_name = 'SrBachchan'

MATCH (replier)<-[:AUTHOR_SCREEN_NAME]-(z_2:TID{type:'Reply'})-[:REPLYTO_SOURCE_ID]->(r)
WHERE z_1.tid = r.replyto_source_id AND x.author_screen_name<>replier.author_screen_name
WITH count(x.author_screen_name + replier.author_screen_name) AS count,collect(z_2) AS tids,x AS author,replier AS replied
 return author,replied,tids,count
  order by count desc

	 """	
def process(file):

	with open(file) as dum:
		tweetDateInfo = json.load(dum)

	listTweet = []
	for (key,value) in tweetDateInfo.items():
			listTweet.append(value)
	
	query_1(listTweet)
	


def recurDir(filePath):
	
	graph = connect_to_db()

	a = "CREATE CONSTRAINT ON (a:TID) ASSERT (a.tid) IS UNIQUE"
	graph.run(a)
	b = "CREATE CONSTRAINT ON (b:Hashtag) ASSERT (b.hashtag) IS UNIQUE"
	graph.run(b)
	
	d = "CREATE CONSTRAINT ON (d:Mention) ASSERT (d.mention) IS UNIQUE"
	graph.run(d)
	e = "CREATE CONSTRAINT ON (e:Type) ASSERT (e.type) IS UNIQUE"
	graph.run(e)
	
	i = "CREATE CONSTRAINT ON (i:ScreenName) ASSERT (i.author_screen_name) IS UNIQUE"
	graph.run(i)
	
	k = "CREATE CONSTRAINT ON (k:Author_name) ASSERT (k.author_name) IS UNIQUE"
	graph.run(k)
	l = "CREATE CONSTRAINT ON (l:Author_id) ASSERT (l.author_id) IS UNIQUE"
	graph.run(l)
	m = "CREATE CONSTRAINT ON (m:Replyto) ASSERT (m.replyto_source_id) IS UNIQUE"
	graph.run(m)
	
	
	
						

						
	
	print(filePath)
	process(filePath)
	
recurDir('dataset.json')	

