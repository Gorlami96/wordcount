from flask import Flask ,render_template ,request ,jsonify
import requests
import json
from flask_sqlalchemy import SQLAlchemy
import os
import operator
import re
import nltk
from stop_words import stops
from collections import Counter
from bs4 import BeautifulSoup
from rq import Queue
from rq.job import Job
from worker import conn

app = Flask(__name__)
app.config.from_object(os.environ['APP_SETTINGS'])
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

q = Queue(connection = conn)

from models import *

@app.route('/',methods =['GET','POST'])
def index():
    return render_template('index.html')

@app.route('/start',methods=['POST'])
def get_counts():
    data = json.loads(request.data.decode())
    url = data['url']
    job  = q.enqueue_call(func = count_and_save_words , args = (url,), result_ttl = 10000)
    return job.get_id()

@app.route("/results/<job_key>", methods=['GET'])
def get_results(job_key):

    job = Job.fetch(job_key, connection = conn)

    if job.is_finished:
        result = Result.query.filter_by(id=job.result).first()
        results = sorted(
            result.result_no_stop_words.items(),
            key=operator.itemgetter(1),
            reverse=True
        )[:10]
        return jsonify(results)
    else:
        return "Nay!",202

@app.route('/<name>')
def hello_name(name):
    return "Hello {}".format(name)

def count_and_save_words(url):
    errors = []
    
    try:
         r = requests.get(url)
    except:
        errors.append("Unable to get URL. Please make sure it's valid and try again.")
    else:
        raw = BeautifulSoup(r.text ,'html.parser').get_text()
        nltk.data.path.append('./nltk_data')
        tokens = nltk.word_tokenize(raw)
        text = nltk.Text(tokens)

        non_punct = re.compile('.*[A-za-z].*')
        raw_words = [w for w in text if non_punct.match(w)]
        raw_word_count = Counter(raw_words)

        no_stop_words = [w for w in raw_words if w.lower() not in stops]
        no_stop_words_count = Counter(no_stop_words)


        results = sorted(no_stop_words_count.items(),
        key = operator.itemgetter(1),
        reverse = True)
        try:
            result = Result(url = url,
            result_all = raw_word_count,
            result_no_stop_words=no_stop_words_count)
            db.session.add(result)
            db.session.commit()
#            print('Committed successfully to database')
            return result.id
        except:
            errors.append("Unable to add item to database.")
            return {"error":errors}

#print(os.environ['APP_SETTINGS'])

if __name__=='__main__':
    app.run()
