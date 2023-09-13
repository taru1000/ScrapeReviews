# -*- coding: utf-8 -*-

### To install libraries
"""

!pip install scrapy

"""### Importing Libraries"""

import os
from bs4 import BeautifulSoup
import urllib
from urllib.request import Request
#import urllib2
import pandas as pd
from lxml import html
import re
import requests
from string import ascii_uppercase
import random
from itertools import islice, count
from tqdm import tqdm
from scrapy.http import HtmlResponse
import math

"""### Input variables from users"""

path = r'/content/drive/MyDrive/GITHUB/PROJECT1' ## destination path location to store final files
comp_folder ='BP' ## company's name
comp_list = ['BP'] ## exact name of the company which is mentioned in Indeed URL after 'cmp'
# For eg: http://www.indeed.com/cmp/BP/reviews?fcountry=ALL&start=0

def build_url(url, company_name):
    return url + '/cmp/' + company_name + '/reviews?fcountry=ALL&start='

def get_reviews(review_url, comp, path):
    review_df = pd.DataFrame()
    review_rating, review_title,reviewer_job, reviewer_location, review_text, pro_review_text, con_review_text, review_date, former= [],[],[],[],[],[],[],[],[]
    WLB, CB, JSA, MNT, JC = [], [], [], [], []
    i=4
    try:
        itr = int(review_url[review_url.find('&start=')+7:])
        url = urllib.request.urlopen(review_url)
        soup = BeautifulSoup(url,"lxml")

        containers=soup.find_all('div',{'class':'cmp-Review-container'})
        print(len(containers))

        for c in containers:
            try:
                review_rating.append(c.find('div', {'class': 'cmp-ReviewRating-text'}).text)
            except:
                review_rating.append('NA')
            try:
                review_title.append(c.find('div', {'class': 'cmp-Review-title'}).text)
            except:
                review_title.append('NA')
            try:
                reviewer_job.append(c.find('span', {'class':'cmp-ReviewAuthor'}).contents[1].text)
                former.append(c.find('span', {'class':'cmp-ReviewAuthor'}).contents[4])
                i=4
            except:
                try:
                   reviewer_job.append(c.find('span', {'class':'cmp-ReviewAuthor'}).contents[1])
                   former.append(c.find('span', {'class':'cmp-ReviewAuthor'}).contents[5])
                   i=5
                except:
                   reviewer_job.append('NA')
                   former.append('NA')
            try:
                reviewer_location.append(c.find('span', {'class':'cmp-ReviewAuthor'}).contents[i+3].text)
            except:
                try:
                   reviewer_location.append(c.find('span', {'class':'cmp-ReviewAuthor'}).contents[i+4])
                except:
                    reviewer_location.append('NA')
            try:
                review_text.append(c.find('div', {'class':'cmp-Review-text'}).text)
            except:
                review_text.append('NA')
            try:
                pro_review_text.append(c.find('div', {'class':'cmp-ReviewProsCons-prosText'}).text)
            except:
                pro_review_text.append('NA')
            try:
                con_review_text.append(c.find('div', {'class':'cmp-ReviewProsCons-consText'}).text)
            except:
                con_review_text.append('NA')
            try:
                review_date.append(c.find('span', {'class':'cmp-ReviewAuthor'}).contents[-1])
            except:
                review_date.append('NA')
            try:
                WLB.append(c.find_all('div', {'class': 'cmp-RatingStars-starsFilled'})[1]['style'][6:])
            except:
                WLB.append('NA')
            try:
                CB.append(c.find_all('div', {'class': 'cmp-RatingStars-starsFilled'})[2]['style'][6:])
            except:
                CB.append('NA')
            try:
                JSA.append(c.find_all('div', {'class': 'cmp-RatingStars-starsFilled'})[3]['style'][6:])
            except:
                JSA.append('NA')
            try:
                MNT.append(c.find_all('div', {'class': 'cmp-RatingStars-starsFilled'})[4]['style'][6:])
            except:
                MNT.append('NA')
            try:
                JC.append(c.find_all('div', {'class': 'cmp-RatingStars-starsFilled'})[5]['style'][6:])
            except:
                JC.append('NA')

        df = pd.DataFrame(list(zip(review_rating, review_title,reviewer_job, reviewer_location, review_text, pro_review_text, con_review_text, review_date, former,WLB, CB, JSA, MNT, JC)))
        df.columns=['Review_Rating', 'Review_Title', 'Job_Title', 'Job_Location', 'Review_Text', 'Pro_Review', 'Con_Review', 'Date_of_Review', 'Former_employee', 'Job_Work_Life_Balance', 'Compensation_Benefits', 'Job_Security_Advancement', 'Management', 'Job_Culture']
        df['Company'] = comp
        review_df = review_df.append(df)
        review_df = review_df.drop_duplicates()
        review_df.to_excel(path + '/review_data/indeed_review_' + comp + str(itr) + '.xlsx', index=False)
    except:
        print(review_url)

## Start of the code
if __name__=='__main__':
  base_url = "http://www.indeed.com"
  for each in range(len(comp_list)):
      company = comp_list[each]
      #max_pages = page[each]
      base = build_url(base_url,company)
      review_url = base + '0'
      url = urllib.request.urlopen(review_url)
      soup = BeautifulSoup(url,"lxml")
      max_pages = math.ceil(pd.to_numeric(re.sub('[^0-9]+','',soup.find('div',{'class':'cmp-ReviewsCount'}).text))/20)

      page_range = list(range(0, max_pages))
      review_urls = [f'{base}{str(x*20)}' for \
                x in page_range]

      from joblib import parallel_backend, Parallel, delayed
      gen = count(0, 1)
      with parallel_backend('threading', n_jobs=8):
          Parallel()(delayed(get_reviews)(review_url, comp=company,path=path + '/' + comp_folder) for review_url in tqdm(review_urls, position=0, leave=True))

      from glob import glob
      file_list = glob(path + '/' + comp_folder + '/review_data/indeed_review_' + company + '*.xlsx')
      df_list = [pd.read_excel(file) for file in file_list]
      complete_df = pd.concat(df_list, axis=0).drop_duplicates()
      complete_df.to_excel(path + '/' + comp_folder + '/Data/' + company + '_indeed_reviews_complete.xlsx', index=False)

