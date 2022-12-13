import pandas as pd
import json
import csv
from elasticsearch import Elasticsearch, helpers
from rank_bm25 import BM25Okapi


es = Elasticsearch(HOST='https://localhost', PORT=9200)

def start(): 

  #Erwthsh apo ton xrhsth
  global user_request
  global query_body
  user_request = input('Search Movie: ')

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

  

def index_and_data():
#Create movies index and import data function
  if es.indices.exists(index="movies") == False:
      es.indices.create(index="movies")
      with open('/home/kyr/Desktop/movies.csv', encoding="utf8") as f:
          reader = csv.DictReader(f)
          helpers.bulk(es, reader, index='movies')


#Search ES function
def search_es():
  results = es.search(index="movies", body=query_body)
  df = pd.DataFrame(results)

  results_titloi = []
  for hit in df["hits"]["hits"]:
      a = hit["_source"]["title"]
      results_titloi.append(a.lower())

  tokenized_results = [doc.split(' ') for doc in results_titloi]
  bm25 = BM25Okapi(tokenized_results)

  query = user_request.lower()
  tokenized_query = query.split(' ')

  doc_scores = sorted(bm25.get_scores(tokenized_query), reverse=True)

  apotelesmata = list(map(lambda x,y:(y),doc_scores,results_titloi))
  for i in apotelesmata:
    print(i.title())


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
