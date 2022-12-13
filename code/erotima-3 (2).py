import pandas as pd
import numpy as np
import json
import csv
from elasticsearch import Elasticsearch, helpers
from rank_bm25 import BM25Okapi
from statistics import mean
from collections import defaultdict
from operator import itemgetter
from sklearn.cluster import KMeans
from matplotlib import pyplot as plt 

#syndesh me elasticsearch
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


#An exei vathmologisei genre
def hasWatched(mo_eidous,eidos):
    for i in range(671):
      if i+1 == mo_eidous[i][0]:
        continue
      else:
        mo_eidous.insert(i,[i+1,0,eidos])

#Na vazei sto DataFrame gia to kmeans
def add_df(mo_eidous,eidos,dataframe):
    temp = []
    for i in mo_eidous:
      temp.append(i[1])
    dataframe[eidos] = temp

#briskei se poion cluster einai o user poy anazhtoyme
def findcluster(user,cluster1,cluster2,cluster3,cluster4,cluster5,cluster6):
    for i in range(len(cluster1[0])):
      if user == cluster1[0][i]:
        return cluster1
    for i in range(len(cluster2[0])):
      if user == cluster2[0][i]:
        return cluster2
    for i in range(len(cluster3[0])):
      if user == cluster3[0][i]:
        return cluster3
    for i in range(len(cluster4[0])):
      if user == cluster4[0][i]:
        return cluster4
    for i in range(len(cluster5[0])):
      if user == cluster5[0][i]:
        return cluster5
    for i in range(len(cluster6[0])):
      if user == cluster6[0][i]:
        return cluster6

#Na xwrizei tis tainies ana eidos
def genre_div(eidos, lista, nealista, diction, mo):
    for i in range(len(lista)):
        if (eidos in lista[i][3]):
            nealista.append((lista[i][0],lista[i][2]))
        else:
            continue
    for user,rating in nealista:
        diction[user].append(rating)
    for i in diction.keys():
        x=mean(diction[i])
        mo.append([i,x,eidos])

#Search ES function
def search_es():    
    results = es.search(index="movies", body=query_body)
    df = pd.DataFrame(results)

    ratings = pd.read_csv('/home/kyr/Desktop/ratings.csv')
    movies = pd.read_csv('/home/kyr/Desktop/movies.csv')

    results_titloi = []
    for hit in df["hits"]["hits"]:
        a = hit["_source"]["title"]
        results_titloi.append(a.lower())

    results_id = []
    for hit in df["hits"]["hits"]:
        a = hit["_source"]["movieId"]
        results_id.append(int(a))

    #pairnoyme apo ta csv arxeia ta dedomena poy 8eloyme
    #kai ta bazoyme se listes 
    list_ratings = []
    list_ratings = ratings['rating']

    list_movies = []
    list_movies = ratings['movieId']

    list_users =[]
    list_users = ratings['userId']

    list_genres = []
    list_genres= movies['genres']
    
    list_id = []
    list_id = movies['movieId']

    #map me movie id kai ratings
    teest1 = list(map(lambda x,y,z:(x,y,z),list_users,list_movies,list_ratings))
    teest2 = list(map(lambda x,y:(int(x),y),list_id,list_genres))
    kathgories=[]
    
    for i in range(len(teest1)):
      for j in range(len(teest2)):
        if (teest1[i][1] == teest2[j][0]):
          kathgories.append(teest2[j][1])
          break
    
    sunolikh_lista = list(map(lambda x,y,z,p:(x,y,z,p),list_users,list_movies,list_ratings,kathgories))
    map123 = map(lambda x,y:(x,y),list_movies,list_ratings)
    list_map123 = list(map123)
    
    d = defaultdict(list)
    for movie, rating in list_map123:
        d[movie].append(rating)

   
    mesoi_oroi=[]

    for i in results_id:
      if(d[int(i)]!=[]):
        mesoi_oroi.append((int(i),mean(d[int(i)])))
      else:
        mesoi_oroi.append((int(i),0))
    
    title_id = list(map(lambda x,y:[str(x),int(y)],results_titloi,results_id))
    
    user,user2=[],[]
    for i in range(len(ratings)):
      user.append(ratings.movieId[i])
      user2.append(ratings.rating[i])
    
    
    user = ratings.movieId[ratings.userId==user_id]
    user2 = ratings.rating[ratings.userId==user_id]
    user_info = list(map(lambda x,y:[int(x),int(y)],user,user2))

    
    user_ratings = {}


    for i in range(len(user_info)):
      for j in range(len(title_id)):
        if title_id[j][1] == user_info[i][0]:
          x=float(user_info[i][1])
          y=int(title_id[j][1])
          user_ratings[y] = x
        else:
          continue 
    

    
    for j in range(len(title_id)):
      if title_id[j][1] not in user_ratings.keys():
        y=int(title_id[j][1])
        x=0
        user_ratings[y] = x
      else:
        continue
    
  
    users = []
    for i in range(1,672):
      users.append(i)

    telikoi_mo = pd.DataFrame(users)
    telikoi_mo.columns = ['Users']


    adventure=defaultdict(list)
    adventure_mo = []
    test1 = []
    
    genre_div('Adventure', sunolikh_lista, test1, adventure, adventure_mo)
    hasWatched(adventure_mo,'Adventure')
    add_df(adventure_mo,'Adventure',telikoi_mo)
    

    action=defaultdict(list)
    action_mo = []
    test2 = []

    genre_div('Action', sunolikh_lista, test2, action, action_mo)
    hasWatched(action_mo,'Action')
    add_df(action_mo,'Action',telikoi_mo)


    animation=defaultdict(list)
    animation_mo = []
    test3 = []

    genre_div('Animation', sunolikh_lista, test3, animation, animation_mo)
    hasWatched(animation_mo,'Animation')
    add_df(animation_mo,'Animation',telikoi_mo)

    
    comedy=defaultdict(list)
    comedy_mo = []
    test4 = []
    
    genre_div('Comedy', sunolikh_lista, test4, comedy, comedy_mo)
    hasWatched(comedy_mo,'Comedy')
    add_df(comedy_mo,'Comedy',telikoi_mo)

    
    children=defaultdict(list)
    children_mo = []
    test5 = []
    
    genre_div('Children', sunolikh_lista, test5, children, children_mo)
    hasWatched(children_mo,'Children')
    add_df(children_mo,'Children',telikoi_mo)


    crime=defaultdict(list)
    crime_mo = []
    test6 = []

    genre_div('Crime', sunolikh_lista, test6, crime, crime_mo)
    hasWatched(crime_mo,'Crime')
    add_df(crime_mo,'Crime',telikoi_mo)


    drama=defaultdict(list)
    drama_mo = []
    test7 = []

    genre_div('Drama', sunolikh_lista, test7, drama, drama_mo)
    hasWatched(drama_mo,'Drama')
    add_df(drama_mo,'Drama',telikoi_mo)


    fantasy=defaultdict(list)
    fantasy_mo = []
    test8 = []

    genre_div('Fantasy', sunolikh_lista, test8, fantasy, fantasy_mo)
    hasWatched(fantasy_mo,'Fantasy')
    add_df(fantasy_mo,'Fantasy',telikoi_mo)

    
    documentary=defaultdict(list)
    documentary_mo = []
    test9 = []

    genre_div('Documentary', sunolikh_lista, test9, documentary, documentary_mo)
    hasWatched(documentary_mo,'Documentary')
    add_df(documentary_mo,'Documentary',telikoi_mo)


    horror=defaultdict(list)
    horror_mo = []
    test10 = []

    genre_div('Horror', sunolikh_lista, test10, horror, horror_mo)
    hasWatched(horror_mo,'Horror')
    add_df(horror_mo,'Horror',telikoi_mo)

    
    thriller=defaultdict(list)
    thriller_mo = []
    test11 = []

    genre_div('Thriller', sunolikh_lista, test11, thriller, thriller_mo)
    hasWatched(thriller_mo,'Thriller')
    add_df(thriller_mo,'Thriller',telikoi_mo)

    
    romance=defaultdict(list)
    romance_mo = []
    test12 = []
    
    genre_div('Romance', sunolikh_lista, test12, romance, romance_mo)
    hasWatched(romance_mo,'Romance')
    add_df(romance_mo,'Romance',telikoi_mo)

    
    mystery=defaultdict(list)
    mystery_mo = []
    test13 = []
    
    genre_div('Mystery', sunolikh_lista, test13, mystery, mystery_mo)
    hasWatched(mystery_mo,'Mystery')
    add_df(mystery_mo,'Mystery',telikoi_mo)

    
    scifi=defaultdict(list)
    scifi_mo = []
    test14 = []
    
    genre_div('Sci-Fi', sunolikh_lista, test14, scifi, scifi_mo)
    hasWatched(scifi_mo,'Sci-Fi')
    add_df(scifi_mo,'Sci-Fi',telikoi_mo)

    
    imax=defaultdict(list)
    imax_mo = []
    test15 = []
    
    genre_div('IMAX', sunolikh_lista, test15, imax, imax_mo)
    hasWatched(imax_mo,'IMAX')
    add_df(imax_mo,'IMAX',telikoi_mo)

    
    war=defaultdict(list)
    war_mo = []
    test16 = []
    
    genre_div('War', sunolikh_lista, test16, war, war_mo)
    hasWatched(war_mo,'War')
    add_df(war_mo,'War',telikoi_mo)

    
    musical=defaultdict(list)
    musical_mo = []
    test17 = []
    
    genre_div('Musical', sunolikh_lista, test17, musical, musical_mo)
    hasWatched(musical_mo,'Musical')
    add_df(musical_mo,'Musical',telikoi_mo)

    
    western=defaultdict(list)
    western_mo = []
    test18 = []
    
    genre_div('Western', sunolikh_lista, test18, western, western_mo)
    hasWatched(western_mo,'Western')
    add_df(western_mo,'Western',telikoi_mo)

    
    filmnoir=defaultdict(list)
    filmnoir_mo = []
    test19 = []

    genre_div('Film-Noir', sunolikh_lista, test19, filmnoir, filmnoir_mo)
    filmnoir_mo.append((671,0,'Film-Noir'))
    hasWatched(filmnoir_mo,'Film-Noir')
    add_df(filmnoir_mo,'Film-Noir',telikoi_mo)


    # Convert DataFrame to matrix
    mat = telikoi_mo.values

    # Using sklearn
    km = KMeans(n_clusters=6)
    X=mat
    y_kmeans = km.fit_predict(X)

    #visualizing the clusters
    plt.scatter(X[y_kmeans==0, 0], X[y_kmeans==0, 1],s=100, c="red", label="Cluster 1")
    plt.scatter(X[y_kmeans==1, 0], X[y_kmeans==1, 1],s=100, c="blue", label="Cluster 2")
    plt.scatter(X[y_kmeans==2, 0], X[y_kmeans==2, 1],s=100, c="green", label="Cluster 3")
    plt.scatter(X[y_kmeans==3, 0], X[y_kmeans==3, 1],s=100, c="cyan", label="Cluster 4")
    plt.scatter(X[y_kmeans==4, 0], X[y_kmeans==4, 1],s=100, c="magenta", label="Cluster 5")
    plt.scatter(X[y_kmeans==5, 0], X[y_kmeans==5, 1],s=100, c="black", label="Cluster 6")
   
    #plot the centroid 
    plt.scatter(km.cluster_centers_[:,0],km.cluster_centers_[:,1],s=300,c="Yellow",label="Centroids")
    plt.title("Clusters of Users")
    plt.xlabel("Genre")
    plt.ylabel("Rating")
    plt.show()

    #Labels of each point
    km.labels_

    #Pythonic way to get the indices of the points for each corresponding cluster
    mydict = {i: np.where(km.labels_ == i)[0] for i in range(km.n_clusters)}

    # Transform this dictionary into list (if you need a list as result)
    cluster1,cluster2,cluster3,cluster4,cluster5,cluster6 = [],[],[],[],[],[]
    for key, value in mydict.items():
        if key==0:
          cluster1.append(value)
        elif key==1:
          cluster2.append(value)
        elif key==2:
          cluster3.append(value)
        elif key==3:
          cluster4.append(value)
        elif key==4:
          cluster5.append(value)
        else:
          cluster6.append(value)


    which_cluster = findcluster(user_id,cluster1,cluster2,cluster3,cluster4,cluster5,cluster6)
    
    cluster_mo = defaultdict(list)

    for user,movie,rating in teest1:
      for i in which_cluster[0]:
        if user == i:
          cluster_mo[movie].append(rating)

    mesoi_oroi_cluster={}

    for i in cluster_mo.keys():
      if(cluster_mo[int(i)]!=[]):
        mesoi_oroi_cluster[int(i)] = mean(cluster_mo[int(i)])
      else:
        mesoi_oroi_cluster[int(i)] = 0

    
    for key,value in user_ratings.items():
      if value == 0:
        for keys,values in mesoi_oroi_cluster.items():
          if key == keys:
            user_ratings[key] = values
       
    
    #bm25 kai oi nees metrikes toy erotimatos 3
    query = user_request.lower()
    tokenized_results = [doc.split(" ") for doc in results_titloi]
    bm25 = BM25Okapi(tokenized_results)
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
    
    for key,value in user_ratings.items():
      c[key].append(value*0.6)

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