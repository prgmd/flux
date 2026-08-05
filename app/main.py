from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def hello():
    return {'text': 'Hello, Flux'}