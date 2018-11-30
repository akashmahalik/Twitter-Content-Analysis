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







@app.route('/query_1',methods=['POST'])
def q_1():


	query = """ MATCH(x:Author_name)
				WHERE x.author_name = {auth}
				MATCH (x)<-[:AUTHOR_NAME]-()<-[:AUTHOR_ID]-(y)
				RETURN {auth} AS author,y.tweet_text AS text


			"""
	auth = request.form['author']
	graph = connect_to_db()
	result = graph.run(query,auth = auth )
	return  render_template('index.html',output = result,nu = 1)	


@app.route('/query_2',methods=['POST'])
def q_2():


	query = """ 
				MATCH (x)<-[:AUTHOR_NAME]-()<-[:AUTHOR_ID]-()-[:MENTION]->(y)
				WHERE x.author_name = {auth}
				RETURN {auth} AS author,y.mention AS mention


			"""
	auth = request.form['author']
	graph = connect_to_db()
	result = graph.run(query,auth = auth )
	return  render_template('index.html',output = result,nu = 2)		
@app.route('/query_3',methods=['POST'])
def q_3():


	query = """ 
				MATCH  (x)<-[:HASHTAG]-()-[:HASHTAG]->(y) 
 
				WHERE x.hashtag>y.hashtag
				WITH count(x.hashtag+'-' + y.hashtag) AS w,x.hashtag AS e, y.hashtag AS f
				return w AS pair_cnt, e AS hash_1,f AS hash_2
				order by pair_cnt DESC
				LIMIT 20


			"""
	
	graph = connect_to_db()
	result = graph.run(query )

	return  render_template('index.html',output = result,nu = 3)	



@app.route('/query_4',methods=['POST'])
def q_4():


	query = """ 
				MATCH(y:Hashtag) WHERE y.hashtag = {hash}
				MATCH  (x)<-[:MENTION]-()-[:HASHTAG]->(y) 
 
				
				WITH count(x.mention+'-' + y.hashtag) AS w,x.mention AS e, y.hashtag AS f
				return w AS pair_cnt, e AS mention,f AS hash 
				order by pair_cnt DESC
				LIMIT 20


			"""
	hash = request.form["hash"]
	graph = connect_to_db()
	result = graph.run(query ,hash = hash)
	return  render_template('index.html',output = result,nu = 4)	


@app.route('/query_5',methods=['POST'])
def q_5():


	query = """ 
				MATCH (x)<-[:LOCATION]-()-[:HASHTAG]->(y)
				WHERE x.location = {loc}
				RETURN {loc} AS loc,y.hashtag AS hashtag


			"""
	loc = request.form['location']
	graph = connect_to_db()
	result = graph.run(query,loc = loc )
	return  render_template('index.html',output = result,nu = 5)

@app.route('/query_6',methods=['POST'])
def q_6():


	query = """ 
				MATCH(x)-[:RETWEET_SOURCE_ID]->(y)
				 WHERE x.author_id<>y.author_id
				  WITH count(x.author_id + y.author_id) AS q,x,y 
				  return q as count,x.author_id AS X,y.author_id AS Y order by q desc LIMIT 20


			"""

	graph = connect_to_db()
	result = graph.run(query)
	return  render_template('index.html',output = result,nu = 6)		

@app.route('/query_7',methods=['POST'])
def q_7():


	query = """ 
				MATCH(x)-[:RETWEET_SOURCE_ID]->(y)
				 WHERE x.author_id<>y.author_id
				  WITH count(x.author_id + y.author_id) AS q,x,y 
				  return q as count,x.author_id AS X,y.author_id AS Y order by q desc LIMIT 20


			"""

	graph = connect_to_db()
	result = graph.run(query)
	return  render_template('index.html',output = result,nu = 7)		




@app.route('/')	
def main():

	return render_template('index.html')

if __name__=='__main__':
	app.run(debug=True)
	main()