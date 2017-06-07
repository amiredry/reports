__author__ = 'shadowtrader'
import pymysql.cursors
import sendgrid
from elasticsearch import Elasticsearch
import datetime
import cli.app

DEV_ENV = {'es': '10.2.42.222', 'mysql': '10.2.42.88'}
PROD_ENV = {'es': '10.1.77.193', 'mysql': '10.1.77.167'}

msql_connection = pymysql.connect(host=PROD_ENV['mysql'],
                                  user='devRead',
                                  password='Rypkit10',
                                  db='qb_main')


def registered_ytd(end_date):
    date = end_date
    format_day = "%s-%s-%s " % (date.year, date.month, date.day) + ' 23:59:59'
    try:
        with msql_connection.cursor() as cursor:

            # sql = "SELECT COUNT(*) FROM qb_main.users where deleted = false;"
            sql = "SELECT count(*) FROM qb_main.users WHERE (created_on < %s) AND (deleted = false);"
            params = format_day
            cursor.execute(sql, params)
            result = cursor.fetchone()

    finally:
        msql_connection.close()

    return result[0]

class MetricsDay(object):

    def __init__(self, environment, start_date, end_date):
        self.es = Elasticsearch([{'host': environment['es']}])
        self.format_start_day = "%s-%s-%s" % (start_date.year, start_date.month, start_date.day)
        self.format_end_day = "%s-%s-%s" % (end_date.year, end_date.month, end_date.day)
        self.new_user = {}
        self.story_metric = {}
        self.user_metric = {}

    def user_signup(self):

        self.new_user['tradestation'] = 0.0
        self.new_user['esignal'] = 0.0
        self.new_user['qbeats'] = 0.0
        self.new_user['total'] = 0.0

        res = self.es.search(index="user_registrations_alias", body={"query": {"range": {"date": {
            "gte": self.format_start_day,
            "lte": self.format_end_day
        }}}})

        size = res['hits']['total']

        if size:
            res = self.es.search(index="user_registrations_alias", body={"query": {"range": {"date": {
                "gte": self.format_start_day,
                "lte": self.format_end_day
            }}}}, size=size)

            nu_list = ['None'] * res['hits']['total']
            if res['hits']['hits']:

                for j, hit in enumerate(res['hits']['hits']):

                    if "platform_name" in hit["_source"]:
                        nu_list[j] = hit["_source"]["platform_name"]
                    else:
                        nu_list[j] = 'Qbeats'

            self.new_user['tradestation'] = nu_list.count('TradeStation')
            self.new_user['esignal'] = nu_list.count('eSignal')
            self.new_user['qbeats'] = res['hits']['total'] - nu_list.count('TradeStation') - nu_list.count('eSignal')
            self.new_user['total'] = res['hits']['total']

        return self.new_user

    def _story(self):
        res = self.es.search(index="story_purchases_alias", body={"query": {"range": {"date": {
            "gte": self.format_start_day,
            "lte": self.format_end_day
        }}}})

        size = res['hits']['total']

        if size:
            res = self.es.search(index="story_purchases_alias", body={"query": {"range": {"date": {
                "gte": self.format_start_day,
                "lte": self.format_end_day
            }}}}, size=size)

        return res

    def story_purchase(self):
        results = self._story()

        return results['hits']['total']

    def avegs(self):
        story_dict = {}
        user_dict = {}
        self.story_metric['revenue'] = 0.0
        self.story_metric['avg_story_user'] = 0.0
        self.story_metric['avg_amount'] = 0.0
        self.story_metric['user_buyer'] = 0.0
        self.story_metric['avg_revenue_user'] = 0.0

        results = self._story()
        size = results['hits']['total']
        if size:
            for hit in results['hits']['hits']:
                add_story(user_dict, hit["_source"]["user_id"], hit["_source"]["price"])

            for x in user_dict.itervalues():
                self.story_metric['revenue'] += sum(x)

            self.story_metric['user_buyer'] = len(user_dict)
            self.story_metric['avg_story_user'] = results['hits']['total'] / len(user_dict)
            self.story_metric['avg_revenue_user'] = self.story_metric['revenue'] / len(user_dict)
            self.story_metric['avg_amount'] = self.story_metric['revenue'] / results['hits']['total']

        return self.story_metric

def add_story(theIndex, word, pagenumber):
    theIndex.setdefault(word, []).append(pagenumber)

def send(start_date, end_date, m, report_type):
    format_start_day = start_date.strftime('%a, %B %d, %Y')
    format_end_day = end_date.strftime('%B %d')

    sg = sendgrid.SendGridClient(sg_username, sg_password)
    message = sendgrid.Mail()
    message.set_from("amir.edry@qbeats.com")
    if report_type == 'daily':
        message.set_subject('Daily Events %s ' % format_start_day)
        header = 'Daily Events %s' % format_start_day
    else:
        message.set_subject('Weekly update')
        header = 'Week of %s' % start_date.strftime('%B %d') + ' - ' + format_end_day

    message.set_text(" ")
    message.set_html(" ")
    reg = registered_ytd(end_date)
    new_user = m.user_signup()
    story_purchase = m.story_purchase()
    msg = m.avegs()
    # SMTP API
    # ========================================================#
    # Add the recipients
    recipients = ["amir.edry@qbeats.com", "carol.eversen@qbeats.com", "giora.stuchiner@qbeats.com",
                  "dan.conte@qbeats.com", "sergey.lyashenko@qbeats.com", "sara.barek@qbeats.com"
                  ]

    message.smtpapi.set_tos(recipients)
    subs = {
        "%header%": [header] * len(recipients),
        "%registered_users%": [reg] * len(recipients),
        "%new_user_tradestation%": [new_user['tradestation']] * len(recipients),
        "%new_user_total%": [new_user['total']] * len(recipients),
        "%new_user_esignal%": [new_user['esignal']] * len(recipients),
        "%new_user_qbeats%": [new_user['qbeats']] * len(recipients),
        "%story_purchase_total%": [story_purchase] * len(recipients),
        "%story_buyer_total%": [msg['user_buyer']] * len(recipients),
        "%revenue_total%": [msg['revenue']] * len(recipients),
        "%avg_s_u%": [msg['avg_story_user']] * len(recipients),
        "%avg_m_s_u%": [round(msg['avg_revenue_user'], 2)] * len(recipients),
        "%avg_m_s_s%": [round(msg['avg_amount'], 2)] * len(recipients)

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
                "template_id": "8638e7f3-1af0-4b96-a643-928e53ee2223"
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


def generate_date(delta_days):
    date = datetime.datetime.now() - datetime.timedelta(days=delta_days)
    return date


def run(report_type):
    if report_type == 'daily':
        start_day = generate_date(1)
        end_day = generate_date(1)
        m = MetricsDay(PROD_ENV, start_day, end_day)
        send(start_day, end_day, m, 'daily')
    else:
        start_day = generate_date(8)
        end_day = generate_date(2)
        m = MetricsDay(PROD_ENV, start_day, end_day)
        send(start_day, end_day, m, 'weekly')


@cli.app.CommandLineApp
def main(app):
    run(app.params.report_type)


main.add_param("-t", "--report_type", help="What type of report default to daily", default="daily", required=True)

if __name__ == '__main__':
    main.run()
