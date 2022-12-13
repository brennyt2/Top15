#Import Libraries
import re
from urllib import request
import gzip
import shutil
import pandas as pd
from multiprocessing.pool import ThreadPool

#Function to download and extract datasets
def download_url(url):
    # Download process
    #print("downloading: ",url)
    file_title = re.split(pattern='/', string=url)[-1]
    urlrtv = request.urlretrieve(url=url, filename=file_title)
   
    # for ".tsv" to ".csv"
    #title = re.split(pattern=r'\.tsv', string=file_title)[0] +".tsv"
    title = re.split(pattern=r'\.tsv', string=file_title)[0] +".tsv"
    
    # Unzip ".gz" file
    with gzip.open(file_title, 'rb') as f_in:
        with open(title, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

#Dataset URL List
urls = [#"https://datasets.imdbws.com/title.episode.tsv.gz"
        "https://datasets.imdbws.com/title.ratings.tsv.gz"
        #,"https://datasets.imdbws.com/title.akas.tsv.gz"
        ,"https://datasets.imdbws.com/title.basics.tsv.gz"
        #,"https://datasets.imdbws.com/title.crew.tsv.gz"
        #,"https://datasets.imdbws.com/title.principals.tsv.gz"
        #,"https://datasets.imdbws.com/name.basics.tsv.gz"
          ]

results = ThreadPool(2).imap_unordered(download_url, urls)

# Read ".tsv" file for Title Ratings
titleRatingsData = pd.read_csv ('title.ratings.tsv',sep='\\t',engine = 'python',na_values=['\\N'])

#print(title_ratings_data)
#title_ratings = pd.DataFrame(title_ratings_data, columns= ['tconst','averageRating','numVotes'])
#print(title_ratings)

# Read ".tsv" file for Title Basics
titleBasicsData = pd.read_csv ('title.basics.tsv',chunksize=100000, sep='\\t',engine = 'python',na_values=['\\N'], usecols = ['tconst', 'titleType', 'primaryTitle'])
chunkBasicsData = pd.concat(titleBasicsData)

dfMovie = chunkBasicsData.query("titleType == 'movie'")
#print("Line 51: ",df2)
mergedDF = pd.merge(titleRatingsData, dfMovie, on='tconst')
#print(newDF)
#print(newDF['numVotes'] >= 100)
validDF = mergedDF.query('numVotes >= 100')
#print(validDF)

totalVotes = mergedDF['numVotes'].sum()
#print(totalVotes)
numRows = len(validDF)
aveNumVotes = totalVotes/numRows

#print("Total votes: ",totalVotes)
#print("NumRows: ",numRows)
#print("Average NumVotes: ",aveNumVotes)
#validDF['answer'] = validDF('numVotes') / validDF()
#newDF2 = validDF['answer'] = (validDF['numVotes'] / aveNumVotes)

tempDF = validDF.assign(answer = (validDF['numVotes'] / aveNumVotes) * validDF['averageRating'])
newDF3 = tempDF.nlargest(15,'answer')
print(newDF3['primaryTitle'])
