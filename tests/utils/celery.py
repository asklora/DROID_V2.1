import json


def celery_echo(payload: dict):
    print("\nFunction is called.")
    print(json.dumps(payload, sort_keys=True, indent=4))
    return payload
