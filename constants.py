# constants.py

from datetime import datetime

LOG_FILE_ENROLLMENT = datetime.now().strftime("%Y-%m-%d") + "_enrollment.txt"
LOG_FILE_EVENT = datetime.now().strftime("%Y-%m-%d") + "_event.txt"
LOG_FILE_DATA_VALUE_SET = datetime.now().strftime("%Y-%m-%d") + "_dataValueSet.txt"
LOG_FILE_DATA_VALUE_SET_LAST_SYNC = datetime.now().strftime("%Y-%m-%d") + "_dataValueSet_last_sync.txt"
LOG_FILE_EVENT_DATA_VALUE_UPDATE = datetime.now().strftime("%Y-%m-%d") + "_updateEventDataValue.txt"
LOG_FILE_EVENT_ERROR_LOG = datetime.now().strftime("%Y-%m-%d") + "_event_error_log.txt"



