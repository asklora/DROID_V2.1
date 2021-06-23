import slack
from global_vars import SLACK_CHANNEL, SLACK_API, SLACK_TEST_CHANNEL
from dotenv import load_dotenv
from environs import Env

def report_to_slack(message, channel=SLACK_CHANNEL):
    try:
        env = Env()
        load_dotenv()
        debug=env.bool("DROID_DEBUG")
        if debug:
            channel = SLACK_TEST_CHANNEL
            message = "[TEST DB] " + message

        client = slack.WebClient(token=SLACK_API, timeout=30)
        client.chat_postMessage(
            channel=channel,
            text=message)
    except Exception as e:
        print(e)
    