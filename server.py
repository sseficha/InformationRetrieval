from flask import Flask, render_template, jsonify
import os
from flask import request


app = Flask(__name__)

@app.route("/")
def home():
    return render_template("google.html")   # (message=message)


@app.route("/query")
def query_process():
    query = request.args.get('query')
    k = request.args.get('k')
    query = query.split(' ')
    #send query
    return render_template('google.html', data=[{'name':'aaa'}, {'name':'bbb'}])


app.run(debug=True)

# if __name__ == "__main__":
#     app.run(debug=True)