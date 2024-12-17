
from flask import Flask, request

app = Flask(__name__)


@app.route('/qa', methods=['POST'])
def qa():  # put application's code here
    query = request.json


@app.route('/')
def say_hello():
    return 'Hello QA System!'


if __name__ == '__main__':
    app.run()
