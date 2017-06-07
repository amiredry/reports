import pymysql.cursors
import sys
import argparse
import sendgrid
from elasticsearch import Elasticsearch
import datetime
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

DEV_ENV = {'es': '10.2.42.222', 'mysql': '10.2.42.88'}
PROD_ENV = {'es': '10.1.77.193', 'mysql': '10.1.77.167'}

msql_connection = pymysql.connect(host=PROD_ENV['mysql'],
                                  user='devRead',
                                  db='qb_main')


def registered_ytd():
    date = datetime.datetime.now() - datetime.timedelta(days=1)
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
    def __init__(self, environment, date):
        self.es = Elasticsearch([{'host': environment['es']}])
        format_day = "%s-%s-%s" % (date.year, date.month, date.day)
        self.previous_day = format_day

    def user_signup(self):
        res = self.es.search(index="user_registrations_alias", body={"query": {"range": {"date": {
            "gte": self.previous_day,
            "lte": self.previous_day
        }}}})

        new_users = res['hits']['total']
        return new_users

    def user_purchase(self):
        res = self.es.search(index="story_purchases_alias", body={"query": {"range": {"date": {
            "gte": self.previous_day,
            "lte": self.previous_day
        }}}})
        print("Got %d Hits:" % res['hits']['total'])
        for hit in res['hits']['hits']:
            print(hit["_source"]["price"])

    def story_purchase(self):
        res = self.es.search(index="story_purchases_alias", body={"query": {"range": {"date": {
            "gte": self.previous_day,
            "lte": self.previous_day
        }}}})
        size = res['hits']['total']

        res = self.es.search(index="story_purchases_alias", body={"query": {"range": {"date": {
            "gte": self.previous_day,
            "lte": self.previous_day
        }}}}, size=size)
        total_amount_story = 0.0
        user_ind = {}
        avg_stories_users = 0.0
        avg_money_stories_users = 0.0
        avg_money_stories_story = 0.0

        if res['hits']['hits']:
            for hit in res['hits']['hits']:
                add_story(user_ind, hit["_source"]["user_id"], hit["_source"]["price"])

                for x in user_ind.itervalues():
                    total_amount_story += sum(x)

                avg_stories_users = size / len(user_ind)
                avg_money_stories_users = total_amount_story / len(user_ind)
                avg_money_stories_story = total_amount_story / size

        num_new_signup = self.user_signup()

        return {'num_new_signup': num_new_signup, 'num_story_purchased': size,
                'total_amount': total_amount_story,
                'num_users_purchased': len(user_ind), 'a11': avg_stories_users,
                'a12': avg_money_stories_users, 'a13': avg_money_stories_story}


def add_story(theIndex, word, pagenumber):
    theIndex.setdefault(word, []).append(pagenumber)


def send(date, msg):
    formatted_date = date.strftime('%a, %B %d, %Y')

    sg = sendgrid.SendGridClient(sg_username, sg_password)
    message = sendgrid.Mail()
    message.set_from("amir.edry@qbeats.com")
    message.set_subject('%s Daily Events' % formatted_date)
    message.set_text(" ")
    message.set_html(" ")
    reg = registered_ytd()

    # SMTP API
    # ========================================================#
    # Add the recipients
    # message.smtpapi.set_tos([
    #     "amir.edry@qbeats.com", "carol.eversen@qbeats.com", "giora.stuchiner@qbeats.com",
    #     "dan.conte@qbeats.com", "sergey.lyashenko@qbeats.com"
    # ])
    message.smtpapi.set_tos([
        "amir.edry@qbeats.com"
    ])

    subs = {
        "%date%": [
            formatted_date, formatted_date, formatted_date, formatted_date, formatted_date
        ],
        "%registered_users%": [
            reg, reg, reg
        ],
        "%sign_ups%": [
            msg['num_new_signup'], msg['num_new_signup'], msg['num_new_signup'], msg['num_new_signup'],
            msg['num_new_signup']
        ],
        "%story_purchased%": [
            msg['num_story_purchased'], msg['num_story_purchased'], msg['num_story_purchased'],
            msg['num_story_purchased'], msg['num_story_purchased']
        ],
        "%user_purchases%": [
            msg['num_users_purchased'], msg['num_users_purchased'], msg['num_users_purchased'],
            msg['num_users_purchased'], msg['num_users_purchased']
        ],
        "%total_amount%": [
            msg['total_amount'], msg['total_amount'], msg['total_amount'], msg['total_amount'], msg['total_amount']
        ],
        "%avg_s_u%": [
            msg['a11'], msg['a11'], msg['a11'], msg['a11'], msg['a11']
        ],
        "%avg_m_s_u%": [
            round(msg['a12'], 2), round(msg['a12'], 2), round(msg['a12'], 2), round(msg['a12'], 2), round(msg['a12'], 2)
        ],
        "%avg_m_s_s%": [
            round(msg['a13'], 2), round(msg['a13'], 2), round(msg['a13'], 2), round(msg['a13'], 2), round(msg['a13'], 2)
        ],

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
                "template_id": "a06d01c0-e097-4d14-8de5-4497ef641e75"
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


def compose_msg(message):
    msg = {}

    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    msg["subject"] = '%s DAILY Events' % yesterday.strftime('%B %d %Y')

    a = message['num_new_signup']
    b = message['num_story_purchased']
    c = message['num_users_purchased']
    d = message['total_amount']
    e = message['a11']
    f = round(message['a12'], 2)
    g = round(message['a13'], 2)
    h = registered_ytd()

    text = """
    Number of new sign ups {}
    Number of stories purchased {}
    Number of users purchased stories {}
    Total amount of stories purchased ${}

    Average number of stories purchased per purchasing user {}
    Average number of stories purchased per purchasing user {}
    Average dollar value per purchases {}
    """.format(a, b, c, d, e, f, g)

    html = """
        <html>
            <head></head>
            <body>
            <h2>{} Daily report</h2>

            <p>Registered users {}</p>

            <h3 style="color:#404040;">Number of new sign ups <span style="color:#F54B23;">{}</span></h3>
            <h3 style="color:#404040;">Number of stories purchased <span style="color:#F54B23;">{}</span></h3>
            <h3 style="color:#404040;">Number of users purchased stories <span style="color:#F54B23;">{}</span></h3>
            <h3 style="color:#404040;">Total amount of stories purchased <span style="color:#F54B23;">${}</span></h3>

            <br>
            <h4 style="color:#808080;">Averages & Ratios</h4>
            <h3 style="color:#404040;">Average number of stories purchased per purchasing user <span style="color:#F54B23;">{}</span></h3>
            <h3 style="color:#404040;">Average purchase value per user buying stories for the day <span style="color:#F54B23;">{}</span></h3>
            <h3 style="color:#404040;">Average dollar value per purchases <span style="color:#F54B23;">${}</span></h3>

    </body>
    </html>
    """.format(yesterday.strftime('%B %d %Y'), h, a, b, c, d, e, f, g)

    msg["text"] = text
    msg["html"] = html

    return msg


def generate_date(delta_days):
    date = datetime.datetime.now() - datetime.timedelta(days=delta_days)
    return date


def main():
    prev_day = generate_date(1)
    m = MetricsDay(PROD_ENV, prev_day)
    send(prev_day, m.story_purchase())


if __name__ == '__main__':
    main()
