#ex1
from pymongo import MongoClient

def get_mongo_client(host='localhost', port=27017):
    
    client = MongoClient(host, port)
    print(f"Connexion réussie à MongoDB sur {host}:{port}")
    return client

#ex2
from pymongo import MongoClient

def get_mongo_client(host='localhost', port=27017):
    client = MongoClient(host, port)
    return client

def get_all_laureates():
    client = get_mongo_client()

    db = client['nobel'] 
    laureates_collection = db['laureates']
    laureates = list(laureates_collection.find({}))

    client.close()

    return laureates

#ex3

#fonction1

from pymongo import MongoClient

def get_mongo_client(host='localhost', port=27017):
   
    client = MongoClient(host, port)
    return client

def get_laureates_information():
   
    client = get_mongo_client()

    db = client['nobel']  

    laureates_collection = db['laureates']
    laureates = list(laureates_collection.find({}, {"firstname": 1, "surname": 1, "born": 1, "_id": 0}))

    client.close()

    return laureates

#fonction2

def get_prize_categories():
    
    client = get_mongo_client()

    db = client['nobel']  

    laureates_collection = db['laureates']

    categories = laureates_collection.distinct("prizes.category")

    client.close()

    return categories

#ex4

#fonction1

from pymongo import MongoClient

def get_mongo_client(host='localhost', port=27017):
  
    client = MongoClient(host, port)
    return client

def get_category_laureates(category):
    
    client = get_mongo_client()

    db = client['nobel'] 

    laureates_collection = db['laureates']
    laureates = list(
        laureates_collection.find(
            {"prizes.category": category},
            {"firstname": 1, "surname": 1, "prizes.category": 1, "_id": 0}
        )
    )

    client.close()

    return laureates

#fonction2

import re
from pymongo import MongoClient

def get_country_laureates(country):
  
    client = get_mongo_client()

    db = client['nobel'] 

    laureates_collection = db['laureates']

    country_regex = re.compile(country, re.IGNORECASE)

    laureates = list(
        laureates_collection.find(
            {"born": country_regex},
            {"firstname": 1, "surname": 1, "born": 1, "_id": 0}
        )
    )

    client.close()

    return laureates

#ex5

#fonction1

from pymongo import MongoClient

def get_shared_prizes():
  
    client = get_mongo_client()
    db = client['nobel'] 

    laureates_collection = db['laureates']

    shared_prizes_pipeline = [
        {"$unwind": "$prizes"}, 
        {"$group": {
            "_id": {
                "year": "$prizes.year",
                "category": "$prizes.category"
            },
            "laureates": {"$push": {"firstname": "$firstname", "surname": "$surname", "motivation": "$prizes.motivation"}},
            "laureates_count": {"$sum": 1}
        }},
        {"$match": {"laureates_count": {"$gt": 1}}}, 
        {"$project": {
            "_id": 0,
            "year": "$_id.year",
            "category": "$_id.category",
            "laureates": 1,
            "laureates_count": 1
        }}
    ]

    
    shared_prizes = list(laureates_collection.aggregate(shared_prizes_pipeline))

    
    client.close()

    return shared_prizes

#fonction2

def get_shared_prizes_common():

    client = get_mongo_client()

    db = client['nobel'] 

    laureates_collection = db['laureates']

    shared_prizes_common_pipeline = [
        {"$unwind": "$prizes"}, 
        {"$group": {
            "_id": {
                "year": "$prizes.year",
                "category": "$prizes.category",
                "motivation": "$prizes.motivation"
            },
            "laureates": {"$push": {"firstname": "$firstname", "surname": "$surname"}},
            "laureates_count": {"$sum": 1}
        }},
        {"$match": {"laureates_count": 2}}, 
        {"$project": {
            "_id": 0,
            "year": "$_id.year",
            "category": "$_id.category",
            "motivation": "$_id.motivation",
            "laureates": 1,
            "laureates_count": 1
        }}
    ]

    shared_prizes_common = list(laureates_collection.aggregate(shared_prizes_common_pipeline))

    client.close()

    return shared_prizes_common

#ex6

from pymongo import MongoClient

def get_mongo_client(host='localhost', port=27017):
    """
    Fonction pour se connecter à une instance MongoDB.
    """
    client = MongoClient(host, port)
    return client

def get_laureates_information_sorted():
   
    client = get_mongo_client()

    db = client['nobel'] 

    laureates_collection = db['laureates']

   
    laureates = list(laureates_collection.find(
        {},
        {"firstname": 1, "surname": 1, "born": 1, "bornCountry": 1, "_id": 0}
    ).sort([
        ("bornCountry", -1),  
        ("born", 1)           
    ]))

    client.close()

    return laureates
