from pymongo import MongoClient
from typing import List, Dict

def get_mongo_client(host='localhost', port=27017) -> MongoClient:
    client = MongoClient(host, port)
    
    return client

# ex1 
def create_award_year_index(client: MongoClient):
    db = client['nobel']
    laureates_collection = db['laureates']
    index_name = laureates_collection.create_index([('prizes.year', -1)]) 

    return index_name

def get_laureates_year(client: MongoClient, year: int) -> List[Dict]:
    db = client['nobel']
    laureates_collection = db['laureates']
  
    cursor = laureates_collection.find(
        {'prizes.year': year}, 
        {'_id': 0, 'firstname': 1, 'surname': 1, 'prizes': 1}
    )
    laureates = list(cursor) 

    return laureates

# ex2
def create_country_index(client: MongoClient):
    db = client['nobel']
    laureates_collection = db['laureates']
    index_name = laureates_collection.create_index([
        ('bornCountry', 'text'),
        ('diedCountry', 'text')
    ])

    return index_name

def get_country_laureates(client: MongoClient, country: str) -> List[Dict]:
    db = client['nobel']
    laureates_collection = db['laureates']
    query = {
        '$text': {
            '$search': country
        }
    }
    projection = {'_id': 0, 'firstname': 1, 'surname': 1, 'bornCountry': 1, 'diedCountry': 1}
    cursor = laureates_collection.find(query, projection)  
    laureates = list(cursor)

    return laureates

# ex4
def create_year_category_index(client: MongoClient):
    db = client['nobel']
    prizes_collection = db['prizes'] 
    index_name = prizes_collection.create_index([
        ('year', 1),      
        ('category', 1)   
    ], unique=True) 

    return index_name