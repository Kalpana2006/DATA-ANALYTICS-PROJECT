# api/app.py
from fastapi import FastAPI
from pymongo import MongoClient
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=['*'], allow_methods=['*'], allow_headers=['*'])

client = MongoClient('mongodb://localhost:27017')
db = client['traffic']
aggregates = db['aggregates']

class Query(BaseModel):
    location_id: str = None

@app.get('/latest')
def latest():
    docs = list(aggregates.find().sort([('window.end', -1)]).limit(100))
    for d in docs:
        d['_id'] = str(d.get('_id'))
    return docs

@app.post('/by-location')
def by_location(q: Query):
    docs = list(aggregates.find({'location_id': q.location_id}).sort([('window.end', -1)]).limit(100))
    for d in docs:
        d['_id'] = str(d.get('_id'))
    return docs
