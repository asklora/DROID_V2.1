import slack
from global_vars import SLACK_CHANNEL, SLACK_API

def report_to_slack(message, channel=SLACK_CHANNEL):
    print(channel)
    # try:
    #     client = slack.WebClient(token=SLACK_API, timeout=30)
    #     client.chat_postMessage(
    #         channel=channel,
    #         text=message)
    # except Exception as e:
    #     print(e)
    