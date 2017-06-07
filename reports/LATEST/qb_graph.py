from __future__ import division
import sendgrid
from elasticsearch import Elasticsearch
import datetime
import cli.app

DEV_ENV = {'es': '10.2.42.222', 'mysql': '10.2.42.88'}
LOC_ENV = {'es': '127.0.0.1', 'mysql': 'localhost'}
PROD_ENV = {'es': '10.1.77.193', 'mysql': '10.1.77.167'}


def price_histogram(elastic):
    es = elastic
    main_list = []
    res = es.search(index='story', doc_type='story_meta', search_type='count',
                    body={
                        "query": {
                            "bool":
                                {"must": [
                                    {"range": {
                                        "created": {
                                            "gte": "2016-01-03",
                                            "lte": "2016-01-09"}}}
                                ]
                                }
                        },
                        "aggs": {
                            "prices": {
                                "terms": {
                                    "field": "start_price",
                                    "size": 0
                                },

                            }
                        }})
    size = res['aggregations']['prices']['buckets']
    for i in size:
        values = i.values()

        main_list.append(values)

    return main_list


def date_histogram(elastic,start_date, end_date):
    es = elastic
    main_list = []
    res = es.search(index='story', doc_type='story_meta', search_type='count',
                    body={
                        "query": {
                            "bool":
                                {"must": [
                                    {"range": {
                                        "created": {
                                            "gte": start_date,
                                            "lte": end_date}}}
                                ]
                                }
                        },
                        "aggs": {
                            "articles_over_time": {
                                "date_histogram": {
                                    "field": "created",
                                    "interval": "6h",
                                    "format": "yyyy-MM-dd"
                                }

                            }
                        }})
    size = res['aggregations']['articles_over_time']['buckets']
    for i in size:
        values = i.values()

        main_list.append(values)

    return main_list


def price_avg_histogram(elastic, start_date, end_date):
    es = elastic
    res = es.search(index='story', doc_type='story_meta', search_type='count',
                    body={
                        "query": {
                            "bool":
                                {"must": [
                                    {"range": {"created": {"gte": start_date, "lte": end_date}}},
                                    # {"range": {"start_price": {"gt": 0}}},
                                    # {"range": {"price": {"gt": 0}}},
                                ]
                                }
                        },
                        "aggs": {
                            "avg_price": {"avg": {"field": "price"}},
                            "max_price": {"max": {"field": "price"}}
                        }
                    })
    size = res['aggregations']

    return size


def publisher_histogram(elastic, start_date, end_date):
    es = elastic
    res = es.search(index='story', doc_type='story_meta', search_type='count',
                    body={
                        "query": {
                            "bool":
                                {"must": [
                                    {"range": {"created": {"gte": start_date, "lte": end_date}}}

                                ]
                                }
                        },
                        "aggs": {
                            "publisher_count": {"cardinality": {"field": "publisher_id"}}
                        }
                    })
    size = res['aggregations']

    return size

def generate_date(delta_days):
    date = datetime.date.today() - datetime.timedelta(days=delta_days)
    return date


def run(env_type):
    es = Elasticsearch([{'host': DEV_ENV['es']}])
    # data = [[0.154, 5928], [0.01, 2775],  [0.231, 1988], [0.11, 1022], [0.132, 884], [5.5, 476], [0.099, 407], [0.055, 399], [0.549999, 292], [0.066, 194], [0.031, 150], [0.22, 150], [0.352, 147], [0.44, 133], [0.21, 129], [3.245, 125], [0.66, 107], [0.101, 105], [0.025, 100], [0.053, 100], [0.054, 100], [0.082, 100], [0.157, 100], [0.176, 100], [0.179, 100], [0.197, 100], [0.211, 100], [0.232, 100], [0.177, 99], [0.213, 99], [0.204, 98], [0.147, 95], [0.036, 92], [0.275, 89], [0.206, 88], [0.06, 87], [5.489, 82], [0.032, 75], [0.103, 75], [0.238, 73], [0.245, 72], [0.224, 60], [0.113, 59], [0.198, 58], [0.172, 53], [0.203, 52], [0.022, 50], [0.027, 50], [0.035, 50], [0.038, 50], [0.043, 50], [0.047, 50], [0.052, 50], [0.061, 50], [0.062, 50], [0.068, 50], [0.071, 50], [0.079, 50], [0.085, 50], [0.088, 50], [0.093, 50], [0.108, 50], [0.109, 50], [0.119, 50], [0.124, 50], [0.129, 50], [0.135, 50], [0.137, 50], [0.141, 50], [0.152, 50], [0.159, 50], [0.164, 50], [0.18, 50], [0.191, 50], [0.193, 50], [0.195, 50], [0.196, 50], [0.205, 50], [0.217, 50], [0.222, 50], [0.236, 50], [0.24, 50], [0.241, 50], [0.247, 50], [0.034, 49], [0.05, 49], [0.087, 49], [0.168, 49], [0.169, 49], [0.042, 48], [0.083, 48], [0.138, 48], [0.181, 46], [0.17, 44], [1.65, 44], [0.55, 40], [0.249, 34], [0.182, 32], [163.9, 32], [0.175, 31], [0.246, 28], [0.104, 27], [0.274999, 27], [0.237, 23], [0.209, 20], [0.0989999, 19], [0.230999, 19], [0.219, 17], [1.155, 15], [0.171, 14], [0.825, 14], [1.089, 14], [1.43, 14], [0.0549999, 13], [0.145, 12], [0.046, 11], [9.9, 11], [0.153999, 9], [0.0989998, 8], [0.274998, 7], [0.151, 6], [3.41, 5], [6.6, 5], [8.8, 5], [55.0, 5], [73.7, 5], [0.023, 4], [1.595, 4], [0.0989997, 3], [0.131999, 3], [0.341, 3], [0.0549997, 2], [0.098, 2], [0.230997, 2], [0.549998, 2], [2.739, 2], [3.85, 2], [27.5, 2], [0.0549993, 1], [0.0549995, 1], [0.0989992, 1], [0.109999, 1], [0.118, 1], [0.13, 1], [0.153993, 1], [0.153995, 1], [0.189, 1], [0.219999, 1], [0.230992, 1], [0.230993, 1], [0.230998, 1], [0.244, 1], [0.25, 1], [0.250562, 1], [0.262558, 1], [0.26961, 1], [0.274994, 1], [0.290377, 1], [0.297, 1], [0.297013, 1], [0.309692, 1], [0.311488, 1], [0.324617, 1], [0.327836, 1], [0.328753, 1], [0.330447, 1], [0.335993, 1], [0.340203, 1], [0.342315, 1], [0.342774, 1], [0.34982, 1], [0.359125, 1], [0.436785, 1], [0.439495, 1], [0.439999, 1], [0.441485, 1], [0.451752, 1], [0.454416, 1], [0.46, 1], [0.462914, 1], [0.519781, 1], [0.549177, 1], [0.549997, 1], [0.623791, 1], [0.66004, 1], [0.732806, 1], [0.757423, 1], [0.780672, 1], [0.786818, 1], [0.81003, 1], [0.816923, 1], [0.824999, 1], [0.960507, 1], [2.409, 1], [2.431, 1], [2.519, 1], [26.9129, 1], [38.5, 1]]
    # bin = [0]*21
    # print len(bin)
    data = price_histogram(es)
    print data
    # for i in data:
    #     if 0 < i[0] <= 0.05:
    #         bin[0] = bin[0] + [i][0][1]
    #
    #     elif i[0] <= 0.10:
    #         bin[1] = bin[1] + [i][0][1]
    #
    #     elif i[0] <= 0.15:
    #         bin[2] = bin[2] + [i][0][1]
    #
    #     elif i[0] <= 0.20:
    #         bin[3] = bin[3] + [i][0][1]
    #
    #     elif i[0] <= 0.25:
    #         bin[4] = bin[4] + [i][0][1]
    #
    #     elif i[0] <= 0.30:
    #         bin[5] = bin[5] + [i][0][1]
    #
    #     elif i[0] <= 0.35:
    #         bin[6] = bin[6] + [i][0][1]
    #
    #     elif i[0] <= 0.40:
    #         bin[7] = bin[7] + [i][0][1]
    #
    #     elif i[0] <= 0.45:
    #         bin[8] = bin[8] + [i][0][1]
    #
    #     elif i[0] <= 0.50:
    #         bin[9] = bin[9] + [i][0][1]
    #
    #     elif i[0] <= 0.55:
    #         bin[10] = bin[10] + [i][0][1]
    #
    #     elif i[0] <= 0.60:
    #         bin[11] = bin[11] + [i][0][1]
    #
    #     elif i[0] <= 0.65:
    #         bin[12] = bin[12] + [i][0][1]
    #
    #     elif i[0] <= 0.70:
    #         bin[13] = bin[13] + [i][0][1]
    #
    #     elif i[0] <= 0.75:
    #         bin[14] = bin[14] + [i][0][1]
    #
    #     elif i[0] <= 0.80:
    #         bin[15] = bin[15] + [i][0][1]
    #     elif i[0] <= 0.85:
    #         bin[16] = bin[16] + [i][0][1]
    #
    #     elif i[0] <= 0.90:
    #         bin[17] = bin[17] + [i][0][1]
    #     elif i[0] <= 0.95:
    #         bin[18] = bin[18] + [i][0][1]
    #
    #     elif i[0] <= 1.00:
    #         bin[19] = bin[19] + [i][0][1]
    #     else:
    #         bin[20] = bin[20] + [i][0][1]
    # print bin
    # for i in range(3, 10):
    #     start_d = "2016-01-%d" % i
    #     data = price_avg_histogram(es, start_d, start_d)
    #     print i,data['avg_price'], data['max_price']
        # data = publisher_histogram(es, start_d, start_d)
        # print data

@cli.app.CommandLineApp
def main(app):
    run(app.params.environment_type)


main.add_param("-e", "--environment_type", help="What environment to run it on? *Default to Development Environment",
               default="loc", required=True)

if __name__ == '__main__':
    main.run()
