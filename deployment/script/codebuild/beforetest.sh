#!/bin/bash
if [[ '$CODEBUILD_BUILD_SUCCEEDING'=='1' ]]
then 
python manage.py send_slack_message -m 'Connection DB check :white_check_mark:' --settings=config.settings.production 
else
python manage.py send_slack_message -m "Connection DB check fail :x: update stopped" --settings=config.settings.production
fi