from flask import Flask, render_template, jsonify
from Index import Index
from flask import request


app = Flask(__name__)

@app.route("/")
def home():
    return render_template("google.html")   # (message=message)


@app.route("/query")
def query_process():
    query = request.args.get('query')
    k = int(request.args.get('k'))
    query = query.split(' ')
    results = Index.topkDocuments(query)
    print(results)
    results = results[0:k]
    return render_template('google.html', data=results)


app.run(debug=True)

