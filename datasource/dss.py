import sys
import boto3
import requests
import pandas as pd
from json import load
from time import sleep, mktime
from requests import get
from getpass import GetPassWarning
from collections import OrderedDict
from general.date_process import datetimeNow
from general.slack import report_to_slack
from general.sql_output import update_ingestion_count
from global_vars import (
    DSS_PASSWORD,
    DSS_PASSWORD2,
    DSS_PASSWORD3,
    DSS_USERNAME,
    DSS_USERNAME2,
    DSS_USERNAME3,
    REPORT_CORPORATE_ACTION,
    REPORT_HISTORY,
    REPORT_INDEXMEMBER,
    REPORT_EOD,
    URL_Extrations,
    URL_AuthToken,
    URL_Extrations_with_note,
)

""" Getting market data from DSS """


def getAuthToken(report=REPORT_HISTORY):
    """
    Step 1: getting authorization token to access the backend
    """

    print(datetimeNow() + " " + "*** Step 1 Request Authorization Token")
    _header = {}
    _header["Prefer"] = "respond-async"
    _header["Content-Type"] = "application/json; odata.metadata=minimal"
    if(report == REPORT_CORPORATE_ACTION):
        _data = {"Credentials": {"Password": DSS_PASSWORD2, "Username": DSS_USERNAME2}}
    else:
        _data = {"Credentials": {"Password": DSS_PASSWORD, "Username": DSS_USERNAME}}
    resp = requests.post(URL_AuthToken, json=_data, headers=_header)

    if resp.status_code != 200:
        _data = {"Credentials": {"Password": DSS_PASSWORD3, "Username": DSS_USERNAME3}}
        resp = requests.post(URL_AuthToken, json=_data, headers=_header)

    if resp.status_code != 200:
        print(
            datetimeNow()
            + " "
            + "ERROR, Get Token failed with "
            + str(resp.status_code)
        )
        sys.exit(-1)
    else:
        _jResp = resp.json()
        return _jResp["value"]

def get_data_from_reuters(
    start_date, end_date, authToken, jsonFileName, stocks, report
):
    """
    Step 2: Load the barebone T&C JSON HTTP request payload from file
    """

    print(
        datetimeNow()
        + " "
        + "*** Step 2 Load the barebone T&C JSON HTTP request payload from file"
    )
    _token = "Token " + authToken
    _jReqBody = {}
    with open(jsonFileName, "r") as filehandle:
        _jReqBody = load(filehandle, object_pairs_hook=OrderedDict)
        # _jReqBody=load( filehandle )
    # loadTicker(exData)

    print(
        datetimeNow()
        + " "
        + "*** Step 3 Append each instrument to the InstrumentIdentifiers array"
    )
    for _inst in stocks:
        if report == REPORT_CORPORATE_ACTION:
            _jReqBody["ExtractionRequest"]["IdentifierList"][
                "InstrumentIdentifiers"
            ].append({"IdentifierType": "Ric", "Identifier": _inst})
            _jReqBody["ExtractionRequest"]["Condition"] = {
                "ReportDateRangeType": "Last",
                "QueryStartDate": start_date + "T00:00:00.000Z",
                "QueryEndDate": end_date + "T00:00:00.000Z",
                "ExcludeDeletedEvents": True,
                "IncludeCapitalChangeEvents": True,
                "IncludeDividendEvents": True,
                "IncludeEarningsEvents": True,
                "IncludeMergersAndAcquisitionsEvents": True,
                "IncludeNominalValueEvents": True,
                "IncludePublicEquityOfferingsEvents": True,
                "IncludeSharesOutstandingEvents": True,
                "IncludeVotingRightsEvents": True,
                "CorporateActionsCapitalChangeType": "CapitalChangeExDate",
                "CorporateActionsDividendsType": "DividendPayDate",
                "CorporateActionsEarningsType": "PeriodEndDate",
                "ShareAmountTypes": []
            }
        elif report == REPORT_HISTORY:
            _jReqBody["ExtractionRequest"]["IdentifierList"][
                "InstrumentIdentifiers"
            ].append({"IdentifierType": "Ric", "Identifier": _inst})
            _jReqBody["ExtractionRequest"]["Condition"] = {
                "QueryStartDate": start_date + "T00:00:00.000Z",
                "QueryEndDate": end_date + "T00:00:00.000Z",
            }
        elif report == REPORT_INDEXMEMBER:
            _jReqBody["ExtractionRequest"]["IdentifierList"][
                "InstrumentIdentifiers"
            ].append({"IdentifierType": "ChainRIC", "Identifier": _inst})
        else:
            _jReqBody["ExtractionRequest"]["IdentifierList"][
                "InstrumentIdentifiers"
            ].append({"IdentifierType": "Ric", "Identifier": _inst})
            _jReqBody["ExtractionRequest"]["Condition"] = {
                "QueryStartDate": start_date + "T00:00:00.000Z",
                "QueryEndDate": end_date + "T00:00:00.000Z",
            }
    _extractReqHeader = makeExtractHeader(_token)
    # Step 4
    print(datetimeNow()+ " " + "*** Step 5 Post the T&C Request to DSS REST server and check response status")
    # resp = requests.post(URL_Extrations, data=None, json=_jReqBody, headers=_extractReqHeader)
    # ======== USING EXTRACT WITH NOTES =================
    resp = requests.post(
        URL_Extrations_with_note, data=None, json=_jReqBody, headers=_extractReqHeader
    )
    if resp.status_code != 200:
        if resp.status_code != 202:
            message = (
                "Error: Status Code:" + str(resp.status_code) + " Message:" + resp.text
            )
            raise Exception(message)

        print(datetimeNow() + " " + "Request message accepted. Please wait...")
        # Get location URL from response message header
        _location = resp.headers["Location"]
        # Polling loop to check request status
        while True:
            resp = get(_location, headers=_extractReqHeader)
            _pollstatus = int(resp.status_code)

            if _pollstatus == 200:
                break
            else:
                print(datetimeNow() + " " + "Status:", resp.headers["Status"])

            # Wait and re-request the status to check if it already completed
            sleep(30)

    print(datetimeNow() + " " + "Response message received")

    # Process Reponse JSON object
    _jResp = resp.json()
    # print("response >> ", _jResp)

    # ========== WITHOUT NOTES  ============
    # data = _jResp["value"]

    # ========== USING NOTES BELOW ============
    data = _jResp["Contents"]

    note = _jResp["Notes"][0].encode("utf-8")
    s3 = boto3.client(
        "s3",
        aws_access_key_id="AKIA2XEOTUNGWEQ43TB6",
        aws_secret_access_key="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",
        region_name="ap-east-1",
    )
    file_name = pd.Timestamp.now()
    epoch = mktime(file_name.timetuple())
    s3_file = "dss-extraction-note/" + str(int(epoch)) + ".txt"
    upload = s3.put_object(Body=note, Bucket="droid-v2-logs", Key=s3_file)

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


def get_data_from_dss(
    start_date, end_date, stocks, jsonFileName, report=REPORT_CORPORATE_ACTION
):
    print(f"Data From {report} Report")
    try:
        # Step 1: Request Authorization Token
        try:
            authToken = getAuthToken(report)
        except GetPassWarning as e:
            print(datetimeNow() + " " + e)
            report_to_slack(
                "{} === Exception AuthToken Error: {}".format(datetimeNow(), e)
            )
        print(datetimeNow() + " " + "Auth Token received")
        
        # Step 2: Request the data with obtained token
        print("get data")
        data = get_data_from_reuters(
            start_date, end_date, authToken, jsonFileName, stocks, report
        )
        update_ingestion_count(source='dss', n_ingest=data.iloc[:,2:].fillna(0).count().count(), dsws=True)
        print(datetimeNow() + " " + "Extraction completed")
        return data
    except Exception as ex:
        print(ex)
        report_to_slack(
            "{} === Exception DSS Ingestion Error: {}".format(datetimeNow(), ex)
        )
