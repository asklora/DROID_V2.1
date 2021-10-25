import os
import json


def celery_echo(payload: dict):
    file_path: str = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        __file__,
    )

    print(f"\n{file_path} is called.")

    print(json.dumps(payload, sort_keys=True, indent=4))
    return payload
