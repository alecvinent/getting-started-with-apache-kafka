from typing import Union

from fastapi import FastAPI

from kafka_client import KafkaClient
import uvicorn

app = FastAPI()

# TODO: to
brokers = ['localhost:9092']
topic = 'mi_tema'

client = KafkaClient(
    brokers=brokers,
    topic=topic
)


#


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/items/{item_id}")
def read_item(item_id: int, q: Union[str, None] = None):
    return {"item_id": item_id, "q": q}


@app.post('/send-message')
async def send_message(data: str):
    client.send_message(data)
    return {"message": "El mensaje se envió con éxito"}


@app.get('/read-messages')
async def read_messages():
    messages = [message for message in client.read_messages()]
    return {"messages": messages}


# TODO: to refactor
uvicorn.run(app=app, host="localhost", port=8000)
