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
import datetime

"""### Input variables from users"""

path = r'/content/drive/MyDrive/GITHUB/PROJECT1' ## destination path location to store final files
comp_folder ='BP' ## company's name
comp_list = ['BP'] ## exact name of the company which is mentioned in Glassdoor URL after 'Reviews'
e_list = ['E9011'] ## exact code of the company which is mentioned in Glassdoor URL after 'Reviews'
# For eg: https://www.glassdoor.co.in/Reviews/BP-Reviews-E9011_P1.htm

def build_url2(url, company_name, url_specific):
    return url + '/Reviews/' + company_name + '-Reviews-' + url_specific + '_P'

def get_reviews(review_url, comp, path):
    review_df = pd.DataFrame()
    review_rating, review_title,reviewer_job, reviewer_location, pro_review_text, con_review_text, review_date= [],[],[],[],[],[],[],[]
    try:
        itr = int(review_url[review_url.find('_P')+2:review_url.rfind('.htm')])
        url = urllib.request.urlopen(Request(review_url,headers=hdr))
        soup = BeautifulSoup(url,"html.parser")
        #soup = BeautifulSoup(url,"lxml")
        containers=soup.findAll('div',{'class':'review-details__review-details-module__topReview'})
        print(len(containers))

        if len(containers) == 0:
            #soup = BeautifulSoup(url,"html.parser")
            soup = BeautifulSoup(url,"lxml")
            containers=soup.findAll('div',{'class':'review-details__review-details-module__topReview'})
            print(len(containers))

        for c in containers:
            try:
                review_title.append(c.find('a', {'class': 'review-details__review-details-module__detailsLink review-details__review-details-module__title'}).text)
            except:
                review_title.append('NA')
            try:
                review_rating.append(c.find('span', {'class': 'review-details__review-details-module__overallRating'}).text)
            except:
                review_rating.append('NA')
            try:
                reviewer_job.append(c.find('span', {'class': 'review-details__review-details-module__employee'}).text)
            except:
                reviewer_job.append('NA')
            try:
                reviewer_location.append(c.find('span', {'class': 'review-details__review-details-module__location'}).text)
            except:
                reviewer_location.append('NA')
            try:
                pro_review_text.append(c.find('span', {'data-test': 'pros'}).text)
            except:
               pro_review_text.append('NA')
            try:
                con_review_text.append(c.find('span', {'data-test': 'cons'}).text)
            except:
               con_review_text.append('NA')
            try:
                review_date.append(c.find('span', {'class': 'review-details__review-details-module__reviewDate'}).text)
            except:
                review_date.append('NA')

        df = pd.DataFrame(list(zip(review_rating, review_title,reviewer_job, reviewer_location, pro_review_text, con_review_text, review_date)))
        df.columns=['Review_Rating', 'Review_Title', 'Job_Title', 'Job_Location', 'Pro_Review', 'Con_Review', 'Date_of_Review']
        df['Company'] = comp
        review_df = review_df.append(df)
        review_df = review_df.drop_duplicates()
        review_df.to_excel(path + '/review_data/glassdoor_review_' + comp + str(itr) + '.xlsx', index=False)
    except:
        print(review_url)

## Start of the code
if __name__=='__main__':
  base_url = "https://www.glassdoor.co.in"
  hdr = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
  sub1 = "of"
  sub2 = "Reviews"
  s=str(re.escape(sub1))
  e=str(re.escape(sub2))
  for each in range(len(comp_list)):
    company = comp_list[each]
    base = build_url2(base_url,company,e_list[each])
    review_url = base + '1.htm'
    url = urllib.request.urlopen(Request(review_url,headers=hdr))
    soup = BeautifulSoup(url,"html.parser")
    max_pages = math.ceil(pd.to_numeric(re.sub(',','',re.findall(s+'(.*)'+e,soup.findAll('div',{'class':'paginationFooter'})[-1].find('span').text)[0].strip()))/10)

    page_range = list(range(1, max_pages+1))
    review_urls = [f'{base}{str(x)}.htm' for \
               x in page_range]

    from joblib import parallel_backend, Parallel, delayed
    gen = count(0, 1)
    with parallel_backend('threading', n_jobs=8):
        Parallel()(delayed(get_reviews)(review_url, comp=company,path=path+'/'+comp_folder) for review_url in tqdm(review_urls, position=0, leave=True))

    from glob import glob
    file_list = glob(path+'/'+comp_folder + '/review_data/glassdoor_review_' + company + '*.xlsx')
    df_list = [pd.read_excel(file) for file in file_list]
    complete_df = pd.concat(df_list, axis=0).drop_duplicates()
    complete_df.to_excel(path+'/'+comp_folder + '/Data/' + company + '_glassdoor_reviews_complete.xlsx', index=False)



