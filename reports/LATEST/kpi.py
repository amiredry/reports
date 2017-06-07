from __future__ import division
import sendgrid
from elasticsearch import Elasticsearch
import datetime, calendar
import cli.app
import pymysql
import pymongo
import sys
from pymongo import MongoClient
from bson.son import SON
import json
from dateutil.relativedelta import relativedelta

DEV_ENV = {'es': '10.2.42.222', 'mysql': '10.2.42.88', 'mongo': '10.2.42.205'}
LOC_ENV = {'es': '127.0.0.1', 'mysql': 'localhost'}
PROD_ENV = {'es': '10.1.77.193', 'mysql': '10.1.77.167', 'mongo': '10.1.77.114'}

#
# def total_publisher(e):
#     environment_type = e
#     msql_connection = pymysql.connect(host=environment_type['mysql'], user='devRead', password='Rypkit10',
#                                       db='qb_main')
#     try:
#         with msql_connection.cursor(pymysql.cursors.DictCursor) as cursor:
#             not_valid_sql = "SELECT account_id FROM qb_main.users WHERE email REGEXP '@qbeats.com' OR " \
#                             "email REGEXP '@mailinator.com';"
#
#             sql = "SELECT publishers.account_id FROM qb_main.publishers " \
#                   "WHERE last_published_on is not NULL;"
#             a = cursor.fetchall()
#             la = [a['account_id'] for a in a]
#
#             cursor.execute(not_valid_sql)
#             b = cursor.fetchall()
#             lb = [b['account_id'] for b in b]
#             c = [item for item in la if item not in lb]
#
#
#     finally:
#         msql_connection.close()
#
#     size = c.__len__()
#     return c


# def total_story(e):
#     total = 0
#     environment_type = e
#     es = Elasticsearch([{'host': environment_type['es']}])
#     res = es.search(index='story', doc_type='story_meta', search_type='count',
#                     body={
#                         "query": {
#                             "bool":
#                                 {"must": [
#                                     {"range": {
#                                         "created": {
#                                             "gte": "2016-01-12",
#                                             "lte": "2016-01-12"}}}
#                                 ]
#                                 }
#                         },
#                         "sort": {"created": {"order": "desc"}},
#                         "aggs": {
#                             "publishers": {"terms": {"field": "publisher_id", "size": 0}}
#                         }})
#
#     a = res['aggregations']['publishers']['buckets']
#     # la = [a['key'] for a in a]
#     lb = total_publisher(PROD_ENV)
#     # c = [item for item in la if item in lb]
#     for i in a:
#         for j in lb:
#             if i['key'] == j:
#                 total = total + i["doc_count"]
#
#     return a


def mongo_tota(e):
    environment_type = e
    client = MongoClient(environment_type['mongo'], 27017)
    db = client.ecquant
    d = datetime.datetime(2016, 11, 12)
    fake_stories = db.news.find({"created_on": {"$lt": d}}).count()
    all_stories = db.news.find({}).count()

    total_real_stories = all_stories - fake_stories

    return total_real_stories


def mongo_total(e, fake_publishers):
    environment_type = e
    f = list(fake_publishers)
    client = MongoClient(environment_type['mongo'], 27017)
    db = client.ecquant
    fake_stories = db.news.find({"publisher_id":{"$in": f}}).count()
    all_stories = db.news.find({}).count()

    total_real_stories = all_stories - fake_stories

    return total_real_stories


def get_publisher(e):
    environment_type = e
    msql_connection = pymysql.connect(host=environment_type['mysql'], user='devRead', password='Rypkit10',
                                      db='qb_main')
    today = datetime.date.today()
    year = today - datetime.timedelta(days=19)
    try:
        with msql_connection.cursor(pymysql.cursors.DictCursor) as cursor:
            sql = "SELECT publishers.account_id, publishers.name  FROM qb_main.publishers where type IN " \
                  "('FREELANCER','INTEGRATED', 'ORGANIZATION') AND last_published_on IS NOT NULL " \
                  "AND created_at <= %s;"
            p = year
            cursor.execute(sql,[p])
            results = cursor.fetchall()

    finally:
        msql_connection.close()
    a = results
    print a
    pub_acct = [a['account_id'] for a in a]
    d = len(pub_acct)
    return pub_acct


def get_fake_acct(e):
    environment_type = e
    msql_connection = pymysql.connect(host=environment_type['mysql'], user='devRead', password='Rypkit10',
                                      db='qb_main')
    try:
        with msql_connection.cursor(pymysql.cursors.DictCursor) as cursor:
            sql = "SELECT account_id FROM qb_main.users WHERE email REGEXP '@qbeats.com' OR " \
                  "email REGEXP '@mailinator.com' OR email REGEXP '@qbeatsinternal.com' " \
                  "OR email REGEXP '@drdrb.net' OR email REGEXP '@n-ix.com';"
            cursor.execute(sql)
            results = cursor.fetchall()

    finally:
        msql_connection.close()
    a = results
    print a
    fake_acct = [a['account_id'] for a in a]
    return fake_acct


def calc_real_pub(e, a, b):
    environment_type = e
    pub = set(a)
    fake = set(b)
    real_pub = pub - fake
    fake_pub = fake.intersection(pub)
    print 'REAL PUBLISHERS:', list(real_pub), len(real_pub)
    print 'FAKE PUBLISHERS:',list(fake_pub), len(fake_pub)

    # try:
    #     with msql_connection.cursor(pymysql.cursors.DictCursor) as cursor:
    #         sql = "SELECT account_id,name FROM qb_main.publishers WHERE account_id IN %s";
    #         param = real_pub
    #         cursor.execute(sql,param)
    #         results = cursor.fetchall()
    #
    # finally:
    #     msql_connection.close()
    # print results
    #
    # k = results
    return [real_pub, fake_pub]





# today = datetime.date.today()
# a = get_publisher(PROD_ENV)
# b = get_fake_acct(PROD_ENV)
# c = calc_real_pub(PROD_ENV, a, b)
# f = c[1]
#
# mongo_total(DEV_ENV, f)
# mongo_tota(DEV_ENV)