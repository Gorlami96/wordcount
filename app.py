from flask import Flask ,render_template ,request
import requests
from flask_sqlalchemy import SQLAlchemy
import os
import operator
import re
import nltk
from stop_words import stops
from collections import Counter
from bs4 import BeautifulSoup

app = Flask(__name__)
app.config.from_object(os.environ['APP_SETTINGS'])
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

from models import Result

@app.route('/',methods =['GET','POST'])
def index():
    errors = []
    results = {}
    if request.method == "POST":
        try:
            url = request.form['url']
            r = requests.get(url)
            print(r.text)
        except:
            errors.append("Unable to get URL. Please make sure it's valid and try again.")
        if r:
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
            except:
                errors.append("Unable to add item to database.")           
    return render_template('index.html' ,errors = errors, results = results)

@app.route('/<name>')
def hello_name(name):
    return "Hello {}".format(name)

#print(os.environ['APP_SETTINGS'])

if __name__=='__main__':
    app.run()
