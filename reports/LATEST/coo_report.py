
from __future__ import division
import sendgrid
from elasticsearch import Elasticsearch
import datetime
import cli.app

DEV_ENV = {'es': '10.2.42.222', 'mysql': '10.2.42.88'}
LOC_ENV = {'es': '127.0.0.1', 'mysql': 'localhost'}
PROD_ENV = {'es': '10.1.77.193', 'mysql': '10.1.77.167'}


class Extractor(object):
    def __init__(self, environment, s_date, e_date):
        self.es = Elasticsearch([{'host': environment['es']}])
        self.start_date = s_date
        self.end_date = e_date

    def get_data(self, es_index, es_type):
        res = self.es.search(index=es_index, doc_type=es_type)

        size = res['hits']['total']

        if size:
            res = self.es.search(index=es_index, doc_type=es_type, size=size)

        return res['hits']['total']


class Metric(object):
    def __init__(self, env_type, start_day, end_day):
        self.es = Elasticsearch([{'host': env_type['es']}])
        self.start_date = start_day
        self.end_date = end_day

    def total_user(self):
        res = self.es.search(index='user', doc_type='user_meta', search_type='count',
                             body={"query": {"range": {"registered": {
                                 "gte": self.start_date,
                                 "lte": self.end_date
                             }}}})
        size = res['hits']['total']
        return size

    def total_story(self):
        res = self.es.search(index='story', doc_type='story_meta', search_type='count',
                             body={
                                 "query": {"range": {"created": {
                                     "gte": self.start_date,
                                     "lte": self.end_date
                                 }}}})

        size = res['hits']['total']
        return size

    def total_publisher(self):

        res = self.es.search(index='user', doc_type='user_meta', search_type='count',
                             body={
                                 "query": {
                                     "filtered": {
                                         "filter": {
                                             "bool":
                                                 {"must":
                                                     [{"term": {
                                                         "is_publisher": True
                                                     }}, {"range": {
                                                         "registered": {
                                                             "gte": self.start_date,
                                                             "lte": self.end_date
                                                         }}}
                                                     ]}
                                         }

                                     }}

                             })

        size = res['hits']['total']

        return size

    def priced_story(self):
        res = self.es.search(index='story', doc_type='story_meta', search_type='count',
                             body={
                                 "query": {
                                     "filtered": {
                                         "filter": {
                                             "bool":
                                                 {"must":
                                                     [{
                                                         "range": {
                                                             "start_price": {
                                                                 "gt": 0
                                                             }}}, {
                                                         "range": {
                                                             "created": {
                                                                 "gte": self.start_date,
                                                                 "lte": self.end_date
                                                             }}}
                                                     ]}
                                         }

                                     }}

                             })

        size = res['hits']['total']

        return size

    def free_story(self):
        res = self.es.search(index='story', doc_type='story_meta', search_type='count',
                             body={
                                 "query": {
                                     "filtered": {
                                         "filter": {
                                             "bool":
                                                 {"must":
                                                     [{"term": {
                                                         "start_price": 0
                                                     }}, {
                                                         "range": {
                                                             "created": {
                                                                 "gte": self.start_date,
                                                                 "lte": self.end_date
                                                             }}}
                                                     ]}
                                         }

                                     }}

                             })

        size = res['hits']['total']
        return size

    def price_change(self):
        res = self.es.search(index='story', doc_type='price_change', search_type='count',

                             body={
                                 "query": {"range": {"start": {
                                     "gte": self.start_date,
                                     "lte": self.end_date
                                 }}}})

        size = res['hits']['total']
        return size

    def shelf_life(self):
        return 0


class Analytics(object):


    def calc_weekly_trend(self, y1, y2):
        per_trend = 0
        trend = y1 - y2

        if trend:
            per_trend = round(100 * trend / y2, 1)

        return per_trend


class Report(object):
    @staticmethod
    def send(start_date, end_date, p, l, t):

        format_end_day = end_date.strftime('%B %d')
        format_start_day = start_date.strftime('%B %d')
        previous_wk, last_wk, trend = p, l, t

        sg = sendgrid.SendGridClient(sg_username, sg_password)
        message = sendgrid.Mail()
        message.set_from("amir.edry@qbeats.com")
        message.set_subject('Weekly update')
        header = 'Week of %s' % format_start_day + ' - ' + format_end_day

        message.set_text(" ")
        message.set_html(" ")

        # SMTP API
        # ========================================================#
        # Add the recipients
        recipients = ["amir.edry@qbeats.com"]

        message.smtpapi.set_tos(recipients)

        subs = {
            "%header%": [header] * len(recipients),

            "%p_total_story%": ['{:,}'.format(previous_wk['total_story'])] * len(recipients),
            "%p_priced_story%": ['{:,}'.format(previous_wk['priced_story'])] * len(recipients),
            "%p_free_story%": ['{:,}'.format(previous_wk['free_story'])] * len(recipients),
            "%p_price_change%": ['{:,}'.format(previous_wk['price_change'])] * len(recipients),
            "%p_shelf_life%": ['-'] * len(recipients),
            "%p_total_publisher%": ['{:,}'.format(previous_wk['total_publisher'])] * len(recipients),

            "%l_total_story%": ['{:,}'.format(last_wk['total_story'])] * len(recipients),
            "%l_priced_story%": ['{:,}'.format(last_wk['priced_story'])] * len(recipients),
            "%l_free_story%": ['{:,}'.format(last_wk['free_story'])] * len(recipients),
            "%l_price_change%": ['{:,}'.format(last_wk['price_change'])] * len(recipients),
            "%l_shelf_life%": ['-'] * len(recipients),
            "%l_total_publisher%": ['{:,}'.format(last_wk['total_publisher'])] * len(recipients),

            "%t_total_story%": [trend['total_story']] * len(recipients),
            "%t_priced_story%": [trend['priced_story']] * len(recipients),
            "%t_free_story%": [trend['free_story']] * len(recipients),
            "%t_price_change%": [trend['price_change']] * len(recipients),
            "%t_shelf_life%": ['-'] * len(recipients),
            "%t_total_publisher%": [trend['total_publisher']] * len(recipients)

        }

        for tag, values in subs.iteritems():
            for value in values:
                message.add_substitution(tag, value)

        message.add_filter('templates', 'template_id', 'd92ae7cd-958b-43ee-bd1b-bc5cd7efe48b')
        message.add_filter('subscriptiontrack', 'enable', '0')

        # SEND THE MESSAGE
        # ========================================================#
        status, msg = sg.send(message)

        print msg

    @staticmethod
    def get_metrics(t, s, e):
        metrics = {}
        m = Metric(t, s, e)
        metrics['total_story'] = m.total_story()
        metrics['total_publisher'] = m.total_publisher()
        metrics['priced_story'] = m.priced_story()
        metrics['free_story'] = m.free_story()
        metrics['price_change'] = m.price_change()
        metrics['shelf_life'] = m.shelf_life()

        return metrics

    def generate_report(self, env_type, start_day, end_day):
        a = Analytics()
        trend = {}
        previous_wk = Report.get_metrics(env_type, start_day, generate_date(8))
        last_wk = Report.get_metrics(env_type, start_day, end_day)

        for metric in previous_wk:
            trend_num = a.calc_weekly_trend(last_wk[metric], previous_wk[metric])
            if trend_num > 0:
                trend[metric] = '<span style="color:#46a800; font-size:16px; ' \
                                'font-weight:normal; font-family:Helvetica Neue,' \
                                'Helvetica,Lucida Grande,tahoma,verdana,' \
                                'arial,sans-serif">{}%</span>'.format(trend_num)
            elif trend_num == 0:
                trend[metric] = '<span style="color:#141823; font-size:16px; font-weight:normal; ' \
                                'font-family:Helvetica Neue,Helvetica,Lucida Grande,tahoma,verdana,arial,sans-serif">' \
                                '{}%</span>'.format(trend_num)
            else:
                trend[metric] = '<span style="color:#da2929; font-size:16px; font-weight:normal; ' \
                                'font-family:Helvetica Neue,Helvetica,Lucida Grande,tahoma,' \
                                'verdana,arial,sans-serif">{}%</span>'.format(trend_num)


        Report.send(generate_date(8), end_day,  previous_wk, last_wk, trend)


def generate_date(delta_days):
    date = datetime.datetime.now() - datetime.timedelta(days=delta_days)
    return date


def run(report_type, env_type):
    r = Report()
    if report_type == 'daily':
        pass
    else:
        start_day = generate_date(1000)
        end_day = generate_date(0)
        r.generate_report(DEV_ENV, start_day, end_day)


@cli.app.CommandLineApp
def main(app):
    run(app.params.report_type, app.params.environment_type)


main.add_param("-t", "--report_type", help="What type of report to run? *Default to daily", default="daily",
               required=True)
main.add_param("-e", "--environment_type", help="What environment to run it on? *Default to Development Environment",
               default="loc", required=True)

if __name__ == '__main__':
    main.run()
