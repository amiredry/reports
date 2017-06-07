__author__ = 'shadowtrader'
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
                             body={
                                 "query": {
                                     "term": {
                                         "user_name": "Alex Filonenko"
                                     }}
                             }
                             )
        size = res['hits']['total']
        return size

    def total_story(self):
        res = self.es.search(index='story', doc_type='story_meta', body={"query": {"range": {"date": {
            "gte": self.start_date,
            "lte": self.end_date
        }}}})

        size = res['hits']['total']
        return size

    def total_publisher(self):
        res = self.es.search(index='user', doc_type='user_meta', body={"query": {"range": {"date": {
            "gte": self.start_date,
            "lte": self.end_date
        }}}})
        size = res['hits']['total']

        if size:
            res = self.es.search(index='user', doc_type='user_meta', size=size)

        return res['hits']['total']

    def priced_story(self):
        return self.es.get_data('story', 'story_meta')

    def free_story(self):
        return self.es.get_data('story', 'story_meta')

    def price_change(self):
        return self.es.get_data('story', 'story_meta')

    def shelf_life(self):
        return self.es.get_data('story', 'story_meta')


class Analytics(object):
    def get_change(self, a, b):
        return '10%'


class Report(object):
    @staticmethod
    def send(start_date, end_date, f, s, t):
        format_start_day = start_date.strftime('%a, %B %d, %Y')
        format_end_day = end_date.strftime('%B %d')
        first_wk, second_wk, trend = f, s, t

        sg = sendgrid.SendGridClient(sg_username, sg_password)
        message = sendgrid.Mail()
        message.set_from("amir.edry@qbeats.com")
        message.set_subject('Weekly update')
        header = 'Week of %s' % start_date.strftime('%B %d') + ' - ' + format_end_day

        message.set_text(" ")
        message.set_html(" ")

        # SMTP API
        # ========================================================#
        # Add the recipients
        recipients = ["amir.edry@qbeats.com"]

        message.smtpapi.set_tos(recipients)
        subs = {
            "%header%": [header] * len(recipients),

            "%total_user%": [new_user['tradestation']] * len(recipients),
            "%total_storyl%": [new_user['total']] * len(recipients),
            "%priced_story%": [new_user['esignal']] * len(recipients),
            "%free_story%": [new_user['qbeats']] * len(recipients),
            "%price_change%": [story_purchase] * len(recipients),
            "%shelf_life%": [msg['user_buyer']] * len(recipients),
            "%total_publisher%": [msg['revenue']] * len(recipients)

        }
        for tag, values in subs.iteritems():
            for value in values:
                message.add_substitution(tag, value)
        # App Filters
        filters = {
            "template": {
                "settings": {
                    "enable": "1",
                    "text/html": ""
                }
            },
            "templates": {
                "settings": {
                    "enable": "1",
                    "template_id": "d92ae7cd-958b-43ee-bd1b-bc5cd7efe48b"
                }
            },
            "subscriptiontrack": {
                "settings": {
                    "enable": "0",
                    "text/plain": "If you would like to unsubscribe and stop receiving these emails click here: <% %>.",
                    "text/html": "<p>If you would like to unsubscribe and stop receiving these emails <% click here %>.</p>"
                }
            }
        }
        for app, contents in filters.iteritems():
            for setting, value in contents['settings'].iteritems():
                message.add_filter(app, setting, value)

        # SEND THE MESSAGE
        # ========================================================#
        status, msg = sg.send(message)

        print msg

    @staticmethod
    def get_metrics(t, s, e):
        metrics = {}
        m = Metric(t, s, e)
        metrics['total_user'] = m.total_user()
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
        first_wk = Report.get_metrics(env_type, start_day, end_day)
        second_wk = Report.get_metrics(env_type, start_day, end_day)

        for metric in first_wk:
            trend[metric] = a.get_change(first_wk[metric], second_wk[metric])

        Report.send(start_day, end_day, first_wk, second_wk, trend)


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
