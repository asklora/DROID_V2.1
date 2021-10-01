
#!/bin/bash
docker restart CeleryBroadcaster
docker exec -i django bash -c "python manage.py send_slack_message -m 'trafic switched :white_check_mark:'"
docker exec -i django bash -c "python manage.py send_slack_message -m 'Server Updated :white_check_mark:'"
