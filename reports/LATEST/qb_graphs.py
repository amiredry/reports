from elasticsearch import Elasticsearch
import pymysql
import json, ast
import datetime, calendar

DEV_ENV = {'es': '10.2.42.222', 'mysql': '10.2.42.88'}

environment_type = DEV_ENV

es = Elasticsearch([{'host': environment_type['es']}])
msql_connection = pymysql.connect(host=environment_type['mysql'], user='devRead', password='Rypkit10', db='qb_main')


def pub_map():
    s = {}
    try:
        with msql_connection.cursor(pymysql.cursors.DictCursor) as cursor:
            sql = "SELECT publishers.account_id, publishers.name  FROM qb_main.publishers where type IN " \
                  "('FREELANCER','INTEGRATED', 'ORGANIZATION','FEED') AND last_published_on IS NOT NULL ;"

            cursor.execute(sql)
            results = cursor.fetchall()

    finally:
        msql_connection.close()

    for i in results:
        s.update(dict([tuple(i.values())]))

    return s


def get_publishers(s, e):
    start_date = s
    end_date = e
    res = es.search(index='story', doc_type='story_meta', search_type='count',
                    body={"fields": ["updated", "created"],
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
                              "Top_publishers": {"terms": {"field": "publisher_id", "size": 0}}
                          }
                          })

    size = res['aggregations']['Top_publishers']['buckets'][0:10]

    return ast.literal_eval(json.dumps(size))


# Weekly Top Publisher
def get_top_publishers(s, e):
    start_date = s
    end_date = e
    publishers = pub_map()
    buckets = get_publishers(start_date, end_date)

    for i in buckets:
        if i['key'] in publishers:
            aa = {'name': publishers[i['key']]}
            i.update(aa)

    return buckets


# Weekly Publisher Distribution
def publishers_distribution(s, e):
    start_date = s
    end_date = e
    res = es.search(index='story', doc_type='story_meta', search_type='count',
                    body={"fields": ["updated", "created"],
                          "query": {
                              "filtered": {
                                  "filter": {
                                      "range": {
                                          "created": {
                                              "gte": start_date,
                                              "lte": end_date}}}
                              }
                          },
                          "aggs": {
                              "Publishers_distribution": {"cardinality": {"field": "publisher_id"}}
                          }
                          })

    size = res['aggregations']['Publishers_distribution']
    d = {'name': start_date.strftime('%A'), 'y': size['value']}
    return d


# Weekly Price distribution
def price_distribution(s, e):
    start_date = s
    end_date = e
    res = es.search(index='story', doc_type='story_meta',
                    body={
                        "query": {
                            "filtered": {
                                "filter": {
                                    "range": {
                                        "created": {
                                            "gte": start_date,
                                            "lte": end_date}
                                    }
                                }
                            }

                        },
                        "aggs": {
                            "weekly_price_distribution": {
                                "terms": {
                                    "field": "price",
                                    "size": 0
                                },

                            }
                        }})

    size = res['aggregations']['weekly_price_distribution']['buckets']

    d = [ {'price':i['key'], 'y':i['doc_count']} for i in size]

    return d


# Average Daily Price
def average_daily_price(s, e):
    start_date = s
    end_date = e
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

                            "avg_price": {"avg": {"field": "price"}}

                        }
                    })
    size = res['aggregations']['avg_price']
    d = {'name': start_date.strftime('%A'), 'y': size['value']}
    return d


# Utility functions
def avg_high_charts_ob(data):
    chart_obj = {
        'chart': {
            'type': 'column'
        },
        'title': {
            'text': 'Average Daily Price'
        },
        'subtitle': {
            'text': 'Week of Jun 1 - Jun 7'
        },
        'xAxis': {
            'categories': ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'],
            'title': {
                'text': 'Day'
            }
        },
        'yAxis': {

            'title': {
                'text': 'Average Price'
            }, 'max': 10,
            'format': '${value:.2f}'

        },
        'legend': {
            'enabled': False
        },
        'plotOptions': {
            'series': {
                'borderWidth': 0,
                'dataLabels': {
                    'enabled': True,
                    'format': '${point.y:.3f}'
                }
            }
        },
        'tooltip': {
            'headerFormat': '<span style="font-size:11px">{series.name}</span><br>',
            'pointFormat': '<span style="color:{point.color}">{point.name}</span>: <b>{point.y:.2f}%</b> of total<br/>'
        },
        'series': [{
            'name': 'Average Price',
            'colorByPoint': True,
            'data': data

        }]
    }
    open('/home/ae/Desktop/highcharts/thefile.js', 'w').write(json.dumps(chart_obj))
    return json.dumps(chart_obj)


def pub_high_charts_ob(data):
    chart_obj = {
        'chart': {
            'type': 'column'
        },
        'title': {
            'text': 'Weekly Publisher Distribution'
        },
        'subtitle': {
            'text': 'Week of Jun 1 - Jun 7'
        },
        'xAxis': {
            'categories': ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'],
            'title': {
                'text': 'Day'
            }
        },
        'yAxis': {

            'title': {
                'text': 'Average Price'
            },
            'format': '{value:.2f}'

        },
        'legend': {
            'enabled': False
        },
        'plotOptions': {
            'series': {
                'borderWidth': 0,
                'dataLabels': {
                    'enabled': True,
                    'format': '{point.y:.2f}'
                }
            }
        },
        'tooltip': {
            'headerFormat': '<span style="font-size:11px">{series.name}</span><br>',
            'pointFormat': '<span style="color:{point.color}">{point.name}</span>: <b>{point.y:.2f}%</b> of total<br/>'
        },
        'series': [{
            'name': 'Number of publishers',
            'colorByPoint': True,
            'data': data

        }]
    }
    open('/home/ae/Desktop/highcharts/publisher_distribution.js', 'w').write(json.dumps(chart_obj))
    return json.dumps(chart_obj)


def generate_date():
    last_monday = datetime.date.today()
    one_day = datetime.timedelta(days=1)
    while last_monday.weekday() != calendar.MONDAY:
        last_monday -= one_day
    print last_monday.strftime('%A, %d-%b-%Y')


def gen_chart_data(o):
    ls_data = []
    for i in o:
        ls_data.append({'name': i['day'], 'y': i['value']})

    return ls_data


def main():
    base = datetime.date(2016, 01, 11)
    date_list = [base + datetime.timedelta(days=x) for x in range(0, 2)]

    # avg_list = [average_daily_price(day, day) for day in date_list]
    # avg_high_charts_ob(pub_list)

    # pub_list = [publishers_distribution(day, day) for day in date_list]
    # pub_high_charts_ob(pub_list)

    # price_dist_list = price_distribution(base, base + datetime.timedelta(days=7))

    top_pub_list = get_top_publishers(base, base + datetime.timedelta(days=7))
    print top_pub_list

if __name__ == '__main__':
    main()
