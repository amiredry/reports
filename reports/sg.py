__author__ = 'aer'
import sendgrid



# CREATE THE SENDGRID MAIL OBJECT
#========================================================#
sg = sendgrid.SendGridClient(sg_username, sg_password)
message = sendgrid.Mail()


message.set_from("amir.edry@qbeats.com")
message.set_subject(" ")
message.set_text(" ")
message.set_html(" ")



# SMTP API
#========================================================#
# Add the recipients
message.smtpapi.set_tos([
    "amir.edry@qbeats.com"
])
subs = {
    "%date%": [
        "Value 1"
    ]
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
    }
}
for app, contents in filters.iteritems():
    for setting, value in contents['settings'].iteritems():
        message.add_filter(app, setting, value)




# SEND THE MESSAGE
#========================================================#
status, msg = sg.send(message)

print msg
