import sys
import requests
import pandas as pd
from json import load
from time import sleep
from requests import get
from getpass import GetPassWarning
from collections import OrderedDict
from general.date_process import datetimeNow
from general.slack import report_to_slack
from global_vars import DSS_PASSWORD, DSS_USERNAME, REPORT_INTRADAY, REPORT_HISTORY, URL_Extrations, URL_AuthToken,URL_Extrations_with_note
# =============================================================================

def getAuthToken():
    # Step 1
    print(datetimeNow()+ " " + "*** Step 1 Request Authorization Token")
    _header = {}
    _header["Prefer"] = "respond-async"
    _header["Content-Type"] = "application/json; odata.metadata=minimal"
    _data = {"Credentials": {"Password": DSS_PASSWORD, "Username": DSS_USERNAME}}
    resp = requests.post(URL_AuthToken, json=_data, headers=_header)
    if resp.status_code != 200:
        print(datetimeNow()+ " " + "ERROR, Get Token failed with " + str(resp.status_code))
        sys.exit(-1)
    else:
        _jResp = resp.json()
        return _jResp["value"]

def get_data_from_reuters(start_date, end_date, authToken, jsonFileName, stocks, report):
    # Step 2
    print(datetimeNow()+ " " + "*** Step 2 Load the barebone T&C JSON HTTP request payload from file")
    _token = "Token " + authToken
    _jReqBody = {}
    with open(jsonFileName, "r") as filehandle:
        _jReqBody = load(filehandle, object_pairs_hook=OrderedDict)
        # _jReqBody=load( filehandle )
    # loadTicker(exData)
    # Step 4
    print(datetimeNow()+ " " + "*** Step 4 Append each instrument to the InstrumentIdentifiers array")
    for _inst in stocks:
        if(report == REPORT_INTRADAY):
            _jReqBody["ExtractionRequest"]["IdentifierList"]["InstrumentIdentifiers"].append(
                {"IdentifierType": "Ric", "Identifier": _inst})
        else:
            _jReqBody["ExtractionRequest"]["IdentifierList"]["InstrumentIdentifiers"].append(
                {"IdentifierType": "Ric", "Identifier": _inst})
            _jReqBody["ExtractionRequest"]["Condition"] = {"QueryStartDate": start_date + "T00:00:00.000Z",
                                                    "QueryEndDate": end_date + "T00:00:00.000Z"}

    _extractReqHeader = makeExtractHeader(_token)
    # Step 5
    print(datetimeNow()+ " " + "*** Step 5 Post the T&C Request to DSS REST server and check response status")
    resp = requests.post(URL_Extrations, data=None, json=_jReqBody, headers=_extractReqHeader)
    # ======== USING EXTRACT WITH NOTES =================
    # resp = requests.post(URL_Extrations_with_note, data=None, json=_jReqBody, headers=_extractReqHeader)
    if resp.status_code != 200:
        if resp.status_code != 202:
            message = "Error: Status Code:" + \
                str(resp.status_code) + " Message:" + resp.text
            raise Exception(message)

        print(datetimeNow()+ " " + "Request message accepted. Please wait...")
        # Get location URL from response message header
        _location = resp.headers["Location"]
        # Polling loop to check request status
        while True:
            resp = get(_location, headers=_extractReqHeader)
            _pollstatus = int(resp.status_code)

            if _pollstatus == 200:
                break
            else:
                print(datetimeNow()+ " " + "Status:", resp.headers["Status"])

            # Wait and re-request the status to check if it already completed
            sleep(30)

    print(datetimeNow()+ " " + "Response message received")

    # Process Reponse JSON object
    _jResp = resp.json()
    data = _jResp["value"]
    
    
    # ========== USING NOTES BELOW ============
    # data = _jResp["Contents"]

    # note = _jResp["Notes"][0]
    # with open("extractions_notes.txt", "w") as file:
    #     file.write(note)
    #     file.close()

    datas = pd.DataFrame.from_dict(data, orient="columns")
    return datas

# =============================================================================

def makeExtractHeader(token):
    _header = {}
    _header["Prefer"] = "respond-async, wait=5"
    _header["Content-Type"] = "application/json; odata.metadata=minimal"
    _header["Accept-Charset"] = "UTF-8"
    _header["Authorization"] = token
    return _header

# =============================================================================

def get_data_from_dss(start_date, end_date, stocks, jsonFileName, report=REPORT_INTRADAY):
    print(f"Data From {report} Report")
    try:
        # Step 1 Request Authorization Token
        try:
            authToken = getAuthToken()
        except GetPassWarning as e:
            print(datetimeNow()+ " " + e)
            report_to_slack("{} === Exception AuthToken Error: {}".format(datetimeNow(), e))
        print(datetimeNow()+ " " + "Auth Token received")
        print("get data")
        data = get_data_from_reuters(start_date, end_date, authToken, jsonFileName, stocks, report)
        print(datetimeNow()+ " " + "Extraction completed")
        return data
    except Exception as ex:
        print(ex)
        report_to_slack("{} === Exception DSS Ingestion Error: {}".format(datetimeNow(), ex))
        
        