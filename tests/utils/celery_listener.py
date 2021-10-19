from importlib import import_module
from celery import app


@app.task(bind=True)
def test_listener(self, data):
    """
    this function is copied from config.celery.
    this will listen to the test celey listener, before we can
    safely use the app's celery listener
    """

    print("listener is called")

    if not "type" in data and not "module" in data and not "payload" in data:
        return {
            "message": "payload error, must have key type , module and payload",
            "received_payload": data,
        }

    if data["type"] == "function":
        module, function = data["module"].rsplit(".", 1)
        mod = import_module(module)
        func = getattr(mod, function)
        res = func(data["payload"])
        if res:
            return res
    elif data["type"] == "invoke":
        module, function = data["module"].rsplit(".", 1)
        mod = import_module(module)
        func = getattr(mod, function)
        func()
