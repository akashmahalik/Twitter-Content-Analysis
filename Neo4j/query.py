from flask import Flask,request,render_template
import os
import json
import sys

from py2neo import Graph, Path,authenticate


app = Flask(__name__)


def connect_to_db():
	authenticate("localhost:7474", "neo4j", "connecttoneo4j")
	graph = Graph()
	return graph







@app.route('/query_7',methods=['POST'])
def q_7():


	query = """
	MATCH (x)<-[:HASHTAG]-(z:TID{type:'Tweet'})-[:MENTION]->(y) WHERE x.hashtag = {hash}
	 WITH count(y.mention) AS count,x as hashtag,y as mention,collect(z.tid) AS TIDs 
	  RETURN hashtag,TIDs,mention,count
	   order by count desc
	    LIMIT 3


			"""

	hash = request.form["hash"]		
	
	graph = connect_to_db()
	web_return=  graph.run(query,hash = hash)
	result = graph.run(query ,hash = hash).data()
	with open('7.json', 'w') as fp:
		json.dump(result, fp)
	return  render_template('index.html',output = web_return,nu = 7)	


@app.route('/query_11',methods=['POST'])
def q_11():


	query = """
	MATCH (x)<-[:AUTHOR_SCREEN_NAME]-(z_1:TID{type:'Tweet'}) WHERE x.author_screen_name = {auth}

MATCH (replier)<-[:AUTHOR_SCREEN_NAME]-(z_2:TID{type:'Reply'})-[:REPLYTO_SOURCE_ID]->(r)
WHERE z_1.tid = r.replyto_source_id AND x.author_screen_name<>replier.author_screen_name
WITH count(x.author_screen_name + replier.author_screen_name) AS count,collect(z_2.tid) AS TIDs,x AS author,replier AS replied
 return author,replied,TIDs,count
  order by count desc

	 """
	auth = request.form['author']
	graph = connect_to_db()
	web_return=  graph.run(query,auth = auth)
	result = graph.run(query ,auth = auth).data()
	with open('11.json', 'w') as fp:
		json.dump(result, fp)
	return  render_template('index.html',output = web_return,nu = 11)		




@app.route('/')	
def main():

	return render_template('index.html')

if __name__=='__main__':
	app.run(debug=True)
	main()