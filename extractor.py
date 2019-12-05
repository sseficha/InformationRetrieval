import re
from bs4 import BeautifulSoup
import requests
import nltk
from nltk.corpus import stopwords

# nltk.download("stopwords")


def html_extractor(url):
    #changed to get html instead of link for now
    # f = open('./samples/'+url, 'r')
    # html = f.read()
    # f.close()

    res = requests.get(url)
    html = res.text

    #get links
    soup = BeautifulSoup(html, 'html.parser')
    links = []
    for link in soup.findAll('a', attrs={'href': re.compile("^http")}):
        links.append(link.get('href'))

    #changed for now
    # soup = BeautifulSoup(html, 'html.parser')
    # links = []
    # for link in soup.findAll('a'):
    #     links.append(link.get('href'))


    #get text
    soup = BeautifulSoup(html, 'html.parser')
    for script in soup(["script", "style","head"]):
        script.decompose()

    body = soup.get_text()

    # lowercase everything
    body = body.lower()

    # Replace all email addresses with white space
    regx = re.compile(r"\b[^\s]+@[^\s]+[.][^\s]+\b")
    body, nemails = regx.subn(repl=" ", string=body)

    # Replace all numbers with white space
    regx = re.compile(r"\b[\d.]+\b")
    body = regx.sub(repl=" ", string=body)


    # Remove all other punctuation (replace with white space)
    regx = re.compile(r"([^\w\s]+)|([_-]+)")
    body = regx.sub(repl=" ", string=body)

    #Replace all newlines and blanklines with special strings
    regx = re.compile(r"\n")
    body = regx.sub(repl=" ", string=body)
    regx = re.compile(r"\n\n")
    body = regx.sub(repl=" ", string=body)

    #Make all white space a single space
    regx = re.compile(r"\s+")
    body = regx.sub(repl=" ", string=body)

    #Remove any trailing or leading white space
    body = body.strip(" ")

    # Remove all useless stopwords
    bodywords = body.split(" ")
    keepwords = [word for word in bodywords if word not in stopwords.words('english')]

    # Stem all words
    # stemmer = SnowballStemmer("english")
    # stemwords = [stemmer.stem(wd) for wd in keepwords]
    # body = " ".join(stemwords)

    # def remove_non_ascii(words):
    #     """Remove non-ASCII characters from list of tokenized words"""
    #     new_words = []
    #     for word in words:
    #         new_word = unicodedata.normalize('NFKD', word).encode('ascii', 'ignore').decode('utf-8', 'ignore')
    #         new_words.append(new_word)
    #     return new_words


    return keepwords, links



