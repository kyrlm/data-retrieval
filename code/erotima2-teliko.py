import pandas as pd
import numpy as np
import json
import csv
from elasticsearch import Elasticsearch, helpers
from rank_bm25 import BM25Okapi
from statistics import mean
from collections import defaultdict
from operator import itemgetter


es = Elasticsearch(HOST='https://localhost', PORT=9200)

def start(): 
  
  #Erwthsh apo ton xrhsth
  global user_request
  global query_body
  global user_id
  user_request = input('Search Movie: ')
  
  try:
    user_id = int(input('Give user\'s ID: '))
  except ValueError:
    print("User\'s id is integer not str, try again")
    user_id = int(input('Give user\'s ID: '))

  while (user_id>671 or user_id==0):
    print("There is no such user, try again")
    user_id = int(input('Give user\'s ID: ')) 

    
    


  #Leksiko gia thn ulopoihsh tou erwthmatos pou tha thesoume sthn ES
  query_body = {
  "size":10000,
    "query": {
      "bool": {
        "must": {
          "match": {      
            "title": user_request
          }
        }
      }
    }
  }


#Create movies index and import data function
def index_and_data():
    if es.indices.exists(index="movies") == False:
        es.indices.create(index="movies")
        with open('/home/kyr/Desktop/movies.csv', encoding="utf8") as f:
            reader = csv.DictReader(f)
            helpers.bulk(es, reader, index='movies')


#Search ES function
def search_es():    
    results = es.search(index="movies", body=query_body)
    df = pd.DataFrame(results)

    ratings = pd.read_csv('/home/kyr/Desktop/ratings.csv')


    results_titloi = []
    for hit in df["hits"]["hits"]:
        a = hit["_source"]["title"]
        results_titloi.append(a.lower())

    results_id = []
    for hit in df["hits"]["hits"]:
        a = hit["_source"]["movieId"]
        results_id.append(int(a))

    list_ratings = []
    list_ratings = ratings['rating']

    list_movies = []
    list_movies = ratings['movieId']

    map123 = list(map(lambda x,y:(x,y),list_movies,list_ratings))

    d = defaultdict(list)

    for movie, rating in map123:
        d[movie].append(rating)

    
    mesoi_oroi=[]

    for i in results_id:
      if(d[int(i)]!=[]):
        mesoi_oroi.append((int(i),mean(d[int(i)])))
      else:
        mesoi_oroi.append((int(i),0))
    
    
    title_id = list(map(lambda x,y:[str(x),int(y)],results_titloi,results_id))
    
    user_movieId = ratings.movieId[ratings.userId==user_id]
    user_movieRating = ratings.rating[ratings.userId==user_id]

    user_info = list(map(lambda x,y:[int(x),int(y)],user_movieId,user_movieRating))
  

    user_ratings = {}

    for i in range(len(user_info)):
      for j in range(len(title_id)):
        if title_id[j][1] == user_info[i][0]:
          x=float(user_info[i][1])
          y=int(title_id[j][1])
          user_ratings[y]=x
        else:
          continue 
    
    for j in range(len(title_id)):
      if title_id[j][1] not in user_ratings.keys():
        y=int(title_id[j][1])
        x=0
        user_ratings[y]=x
      else:
        continue
    

    tokenized_results = [doc.split(" ") for doc in results_titloi]
    bm25 = BM25Okapi(tokenized_results)
    
    query = user_request.lower()
    tokenized_query = query.split(" ")
    doc_scores = bm25.get_scores(tokenized_query)
    mesoi_oroi = sorted(mesoi_oroi)
   
    bm25_scores = list(map(lambda x,y:(int(x),float(y)),results_id,doc_scores))
    bm25_scores = sorted(bm25_scores)

    c = defaultdict(list)
    
    for i,j in bm25_scores:
      c[i].append(j*0.3)

    for i,j in mesoi_oroi:
      c[i].append(j*0.1)
    
    for i,j in user_ratings.items():
      c[i].append(user_ratings[i]*0.6)

    metrikh = []

    for i in results_id:
      metrikh.append((int(i),sum(c[float(i)])))

    metrikh = sorted(metrikh, key=lambda x: x[0])
    title_id = sorted(title_id, key=lambda y: y[1])
    
    times_metrikhs=[]
    titloi_tainiwn=[]

    for i in range(len(metrikh)):
      x=metrikh[i][1]
      times_metrikhs.append(x)

    for i in range(len(title_id)):
      x=title_id[i][0]
      titloi_tainiwn.append(x)
    
    apotelesmata = list(map(lambda x,y:(y,x),titloi_tainiwn,times_metrikhs))
    apotelesmata.sort(key=lambda x:x[0], reverse=True)
    
    for i in apotelesmata:
      print(i[1].title())
    
k=1
while(k==1):
  start()  
  index_and_data()
  try:
    search_es()
  except ZeroDivisionError:
    print("Oops... No such movie in our database\n")
  i = input('If you want to search a movie again press 1. If you want to exit press 0.\n')
  k=int(i)
  if (k==0):
    print("You exited.")


