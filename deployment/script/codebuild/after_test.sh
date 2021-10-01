#!/bin/bash
if [[ '$CODEBUILD_BUILD_SUCCEEDING'=='1' ]]
then 
    python manage.py send_slack_message -m 'Test Success :white_check_mark:' --settings=config.settings.production 
else
    python manage.py send_slack_message -m 'Test Fail :x: rollback update' --settings=config.settings.production
fi