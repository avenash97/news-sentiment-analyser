import pandas as pd
import datetime
import time
import os
import argparse
import io
import numpy
import csv
import ijson
import requests
import simplejson as json
import operator
import urllib.request as urllib2
from goose3 import Goose
from bs4 import BeautifulSoup
from datetime import date, timedelta, datetime, timezone
from io import StringIO
from newsapi import NewsApiClient
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
nltk.download('vader_lexicon')


class NewsProcessor:

	def __init__(self, api_url, API_KEY):
		self.api_url = api_url
		self.API_KEY = API_KEY

	def preprocess(self, article):
		"""preprocessing article by cleaning
		
		Arguments:
			article {String} -- article extracted
		
		Returns:
			String -- cleaned article
		"""
		self.punctuations = '''(){-}[]'"\\<>/@#$%^&*_~’“”—–•'''
		self.cleanedArticle = " "
		self.article = article
		for char in self.article:
			if char not in str(self.punctuations):
				self.cleanedArticle = self.cleanedArticle + char
		self.cleanedArticle = self.cleanedArticle.rstrip('\n')
		self.cleanedArticle = self.cleanedArticle.replace("\n", ' ')
		self.cleanedArticle = self.cleanedArticle.replace("\\u", ' ')
		return self.cleanedArticle

	def dataProcessing(self):
		"""Processing content retrieved for category classification and entites extraction 
		
		Arguments:
			articleRetrieved {String} -- article retrieved
			summarizedContent {String} -- summarized content of article retieved with title
		
		Returns:
			List -- extracted entites and categories
		"""
		categories = []
		self.appendedCategory = self.classify(articleRetrieved, verbose=False)
		self.entityResultList = self.entitiesExtraction(summarizedContent)
		return self.entityResultList, self.appendedCategory
	
	# def writingToJson(articleId, articleTitle, articlePublishedDate, Timestamp, articleURL, entityResultList, appendedCategory, articleExtracted, summarizedContent, resultList):
	# 	"""writes new result to result List in json format
		
	# 	Arguments:
	# 		articleId {Integer} -- Article ID
	# 		articleTitle {String} -- Article Title
	# 		articlePublishedDate {TimeStamp} -- Article Published Date
	# 		Timestamp {TimeStamp} -- Article Retrieved Date
	# 		articleURL {String} -- Article Url
	# 		entityResultList {List} -- List of entites extracted
	# 		appendedCategory {List} -- List of Categories extracted
	# 		articleExtracted {String} -- Extracted article content
	# 		summarizedContent {String} -- Summarized article content with Title
	# 		resultList {List} -- List of appended results
		
	# 	Returns:
	# 		Dictionary, List -- return result and list of results
	# 	"""
	# 	result = {"articleId":articleId, "articleTitle" :articleTitle, "articlePublishedDate":articlePublishedDate,  "articleURL":articleURL, "Timestamp":Timestamp, "category":appendedCategory, "entity":entityResultList, "articleExtracted":articleExtracted, "summarizedContent":summarizedContent}
	# 	resultList.append(result)
	# 	return result, resultList

	def extractGoogleNewsAPIContent(self):
		"""extracting Top News from News API
		
		Arguments:
			api_url {String} -- API Url
			API_KEY {String} -- API Key
		
		Returns:
			List -- List of extracted content
		"""
		dateList = []
		for i in range(1):
			dateList.append(datetime.strftime(datetime.now() - timedelta(i), '%Y-%m-%d'))
		urlList = []
		gnewsInfoList = []
		value = {}
		count = 0
		initialPayload={"apiKey":self.API_KEY,"to":str(dateList[0]), "pageSize":"10","page":"{}".format(i),"language":"en"}
		response = requests.get(self.api_url, params = initialPayload)
		htmlResponse = response.json()
		# pageRange = int(htmlResponse['totalResults']/100)
		pageRange = 2

		for date in dateList:
			for i in range(1,pageRange):# make dynamic variable
				payload={"apiKey":self.API_KEY,"to":str(date), "pageSize":"10","page":"{}".format(i),"language":"en"} #100
				jsonOutput = {}
				response = requests.get(self.api_url, params = payload)
				htmlResponse = response.json()
				jsonOutput["articles"] = []
				for article in htmlResponse['articles']:
					author=article["author"]
					title=article["title"]
					description=article["description"]
					url=urlList.append(article["url"])
					if (article["url"] == 'null'):
						count = count + 1
					publishedAt = article["publishedAt"]
					value['url'] = article["url"]
					value['publishedAt'] = article["publishedAt"]
					value["title"] = article["title"] 
					gnewsInfoList.append(value)
					value={}
		print("urlList length ", len(urlList))
		return gnewsInfoList




def main():
	"""intitial function triggered by Python operator in cloud composer
	"""

	result = {}
	resultList = []
	
	API_KEY="5b37b8a8d50249cfaa792c62c5c6732f"
	api_url = "https://newsapi.org/v2/top-headlines?"
	news = NewsProcessor(api_url, API_KEY)
	gnewsInfoList = news.extractGoogleNewsAPIContent()
	gnewsInfoDataframe = pd.DataFrame(gnewsInfoList)
	
	articleId = 0
	for index, row in gnewsInfoDataframe.iterrows():
		try:
			countNotWritten = 0
			summarizedContent = ''
			summarizedCategoryContent = ''
			articleURL = row['url']
			articlePublishedDate = row['publishedAt']
			articleTitle = row['title']
			entitySet = set()
			g = Goose()
			article = g.extract(url=articleURL)
			extractedTextFromHTML = article.cleaned_text
			articleExtracted = news.preprocess(extractedTextFromHTML)
			articleContent = articleTitle + articleExtracted
			print("articleContent sample words",articleContent[:50])
			sia = SentimentIntensityAnalyzer()
			print("Negative score: ",sia.polarity_scores(articleContent)['neg'])
			print("Positive score: ",sia.polarity_scores(articleContent)['pos'])
			print("Neutral score: ",sia.polarity_scores(articleContent)['neu'])
		except Exception as e:
			print("An exception occurred",e)
			pass
	
	
	# updatingJsonFile(sourceFile)

if __name__ == "__main__":
    main()