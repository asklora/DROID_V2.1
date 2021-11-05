# Logger
Contains a logger class and log template for DROIDv2.

**Usage:**
```
logger.log(*args, *kwargs)

Args:
    task (str): The task that's being done. (e.g. Ingestion)
    subtask (str): The subtask that's being done. (e.g. update_data_dsws_from_dsws)
    start_time (float): Time of when the subtask started in iso format.
    ticker (str): Ticker.
    currency (str): Currency type.
Kwargs:
    end_time (float): Datetime of when the subtask ended in epoch time. If None, task never finished. Defaults to None.
    severity (int): Severity of error. 1-4. Defaults to None.
    error_message (str): Error message. Defaults to None.

```

**Example:**
```
import logger

logger = logger.ESLogger()
logger.log("Ingestion", "update_data_dsws_from_dsws", time.time(), "1337.HK", "HKD", time.time(), "0", str(errorObj))
```

**Requirements**
```
Flask==2.0.2
python_terraform==0.10.1
```

# Contents
```
logging/
┣ README.md
┣ envs.sh
┗ logger.py
```

## **log.json**
The log template. This matches the ES mapping. 

## **logger.py**
Contains the logger object.
