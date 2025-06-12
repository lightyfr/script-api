from fastapi import FastAPI
from pydantic import BaseModel
from scraper import scrape_and_store

app = FastAPI()

class ScrapeRequest(BaseModel):
    university: str
    department: str
    url: str
    selectors: dict

@app.post("/scrape")
def scrape_endpoint(payload: ScrapeRequest):
    try:
        inserted = scrape_and_store(payload.dict())
        return { "status": "success", "inserted": inserted }
    except Exception as e:
        return { "status": "error", "message": str(e) }