import slack
import os

def report_to_slack(message, channel=os.getenv("SLACK_CHANNEL")):
    try:
        client = slack.WebClient(token=os.getenv("SLACK_API"), timeout=30)
        client.chat_postMessage(
            channel=channel,
            text=message)
    except Exception as e:
        print(e)
    