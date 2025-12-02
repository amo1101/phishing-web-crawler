#python .\safe-browsing.py --csv data-dir\iosco2025-09-19\iosco2025-09-19.csv --regulators regulatorDomains2025-10-02-manual.csv

#https://osintteam.blog/phishing-links-to-red-flags-using-googles-safe-browse-apis-to-build-your-own-url-checking-utility-ed44f1ea211b

import os
import sys
import time
import argparse
import dotenv
import traceback
import logging
import itertools
import collections

import requests
import json 
import pandas as pd

import validators
import urllib
import urlextract

from datetime import date, datetime, timezone
from pathlib import Path

def download_csv(csv_dir: Path):
    IOSCO_CSV_URL = "https://www.iosco.org/i-scan/?export-to-csv&NCA_ID=&VALIDATIONDATEEND=&ID=&VALIDATIONDATESTART=&PRODUCTID=&SUBSECTION=main&PAGE=1&CATEGORYID=&KEYWORDS="
    # create the data directory if it is missing
    if csv_dir.is_dir():
        pass
    else:
        csv_dir.mkdir(exist_ok=True)

    #download IOSCO file
    ioscoCSV = "iosco" + date.today().isoformat() + ".csv"
    ioscoCSVPath = Path(csv_dir, ioscoCSV)

    #does the file already exist?
    if ioscoCSVPath.is_file():
        return str(ioscoCSVPath)

    #logger.info(f"Preparing to download IOSCO CSV file from \n{IOSCO_CSV_URL} \nto \n{ioscoCSVPath}")
    try:
        #print("Downloading CSV from IOSCO...")
        response = requests.get(IOSCO_CSV_URL)
        response.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)
        with open(ioscoCSVPath, 'wb') as f:
            f.write(response.content)
        return str(ioscoCSVPath)

    except requests.exceptions.RequestException as e:
        #logger.error(f"Error downloading IOSCO the file: {e}")
        return ""


# https://developers.google.com/safe-browsing/v4/get-started
dotenv.load_dotenv()
GOOGLE_API_KEY = os.getenv('GOOGLE_SAFEBROWSING_API_KEY')
#print(f"GOOGLE_API_KEY: {GOOGLE_API_KEY}")
API_URL = f"https://safebrowsing.googleapis.com/v4/threatMatches:find?key={GOOGLE_API_KEY}"

parser = argparse.ArgumentParser()
parser.add_argument("--csv", help="path to CSV file to analyse, if not provided, download the csv file for today and analyse it", default="")
parser.add_argument("--data_dir", help="output directory", default="data-dir")
parser.add_argument("--regulators", help="CSV file of regulator domains")
args = parser.parse_args()

today = date.today()

#main output dir
data_dir = Path.cwd().joinpath(args.data_dir)
# create the data directory if it is missing
if data_dir.is_dir():
    pass
else:
    data_dir.mkdir(exist_ok=True)

 # use the same folder as the csv file
if args.csv == "":
    newDirPath = Path(data_dir, today.isoformat())
    args.csv = download_csv(newDirPath)
else:
    newDirPath = Path(args.csv).parent

if args.csv == "":
    print('IOSCO csv file not downloaded!')
    sys.exit()

newDirName = Path(args.csv).stem    # use csv file name as prefix
dateFragment = today.isoformat()

#dataPath = Path.cwd().joinpath(data_dir)
#newDirPath = Path(dataPath, newDirName)

loggingFileName = newDirName + "-safebrowsing.log"
loggingFilePath = Path(newDirPath, loggingFileName)

logging.basicConfig(
    level=logging.INFO,
    filename= loggingFilePath,
    encoding="utf-8",
    filemode="w", #overwrite previous logs
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M %z"
)

logger = logging.getLogger(__name__)
if args.csv:
    logger.info(f"Analysing CSV file at: {args.csv}")
if args.regulators:
    logger.info(f"Using Regulator domains list from: {args.regulators}")
logger.info("Starting safe browsing processing for " + dateFragment + "...")


#comment out this to disable the test data size limit and process all data
testDataSize = 100

if 'testDataSize' in globals():
    print("Test Data Size is enabled")
    print(f"testDataSize: {testDataSize}")

#string constants from Google Safe Browsing API
#https://developers.google.com/safe-browsing/v4/lookup-api
MATCHES = "matches"
THREAT_TYPE = "threatType"
PLATFORM_TYPE = "platformType"
THREAT_ENTRY_TYPE = "threatEntryType"
THREAT = "threat"
URL = "url"
CACHE_DURATION = "cacheDuration"
THREAT_ENTRY_METADATA = "threatEntryMetadata"

#column names in IOSCO CSV file 
IOSCO_IDCol = "id"
NCA_URL = "nca_url"
urlCol = "url"
otherurlCol = "other_urls"
comNameCol = "commercial_name"
addInfoCol = 'additional_information'

#column names in output files
IOSCO_ID = "IOSCO_ID"
SOURCE_COLUMN = 'source_column'
RAW_URL = "rawURL"
rawURL = 'rawURL'
TIDY_URL = 'tidyURL'
    
ioscoCSVPath = Path(args.csv)
todaysCSV_df = pd.read_csv(ioscoCSVPath, dtype=str)

if 'testDataSize' in globals():
    todaysCSV_df = todaysCSV_df.head(testDataSize)

#analyse the nca_url column for Regulators' URLs
regulatorURLsList = todaysCSV_df[NCA_URL].tolist()

#read in regulator list
regulatorPath = Path(args.regulators)
regulators_df = pd.read_csv(regulatorPath, dtype=str)
regulatorDomainsList = list(regulators_df.iloc[ :, 0])

logger.info(f"{len(regulatorDomainsList)} regulator domains found")

# get URLs from all relevant fields

def parseURLField(urlField: str) -> list:
    #urls = extractor.find_urls(urlField)
    return extractor.find_urls(urlField)

# process dataframe built from IOSCO CSV into list of tuples of
# URL instances from 4 columns
# each tuple is (IOSCO ID, source column, urlText)
def parseURLCol2(CSV_df) -> list:
    listofFoundURLTuples = [] 
    for row in CSV_df.itertuples(index=True):
        count  = getattr(row, 'Index')
        #if count % 1000 == 0:
        #    print (count, end = "...", flush=True)
        IOSCO_ID = getattr(row, IOSCO_IDCol)
        #url column in IOSCO data
        urlColSource = getattr(row, urlCol)
        urlColURLs = parseURLField(str(urlColSource))
        for url in urlColURLs:
            #foundURLTuple = (IOSCO_ID, urlCol, url.strip())
            listofFoundURLTuples.append((IOSCO_ID, urlCol, url.strip())) 
        #commercial_name column in IOSCO data
        comNameSource = getattr(row, comNameCol)
        comNameColURLs = parseURLField(str(comNameSource))
        for url in comNameColURLs:
            #foundURLTuple = (IOSCO_ID, comNameCol, url.strip())
            listofFoundURLTuples.append((IOSCO_ID, comNameCol, url.strip()))
        #addInfoCol column in IOSCO data
        addInfoSource = getattr(row, addInfoCol)
        addInfoColURLs = parseURLField(str(addInfoSource))
        for url in addInfoColURLs:
            #foundURLTuple = (IOSCO_ID, addInfoCol, url.strip())
            listofFoundURLTuples.append((IOSCO_ID, addInfoCol, url.strip()))
        
        #otherurlCol column in IOSCO data
        # additional processing because items are separated by | and urlextract misses them
        #check comma separated as well
        otherurlSource = getattr(row, otherurlCol)
        list_urlsSources = str(otherurlSource).split("|")
        otherurlColURLs = []
        for urlSource in list_urlsSources:
            otherurlColURLs.extend(parseURLField(str(urlSource)))
        for url in otherurlColURLs:
            #foundURLTuple = (IOSCO_ID, otherurlCol, url.strip())
            listofFoundURLTuples.append((IOSCO_ID, otherurlCol, url.strip()))
    return listofFoundURLTuples

logger.info(f"Starting parsing IOSCO file {ioscoCSVPath} ...")

#https://pypi.org/project/urlextract/
extractor = urlextract.URLExtract()
#URLColList = parseURLCol(todaysCSV_df[urlCol], urlCol) 
#countCSVRows = len(todaysCSV_df)

listofFoundURLTuples = parseURLCol2(todaysCSV_df)
logger.info(f"Number of URL instances found: {len(listofFoundURLTuples)}")
URLTuples_df = pd.DataFrame(listofFoundURLTuples, columns=[IOSCO_ID, SOURCE_COLUMN, rawURL])

#regularise the raw URLs
def tidyRawURL(rawURL: str) -> str:
    tidyURL =''
    try:
        parseResult = urllib.parse.urlparse(rawURL.strip())
        
        #keep the domain only
        if parseResult.scheme != '':
            tidyURL = parseResult.scheme + "://" + parseResult.netloc
            #print(URLtoTest)
                
        if parseResult.scheme == '' and parseResult.netloc == '':
            newPath = parseResult.path.strip("'\"“”: ")
            #keep http as is
            if newPath.startswith('http'):
                tidyURL = newPath
            else: #add https if no scheme present
                tidyURL = 'https' + "://" + newPath

    except Exception as e:
        #print(f"{rawURL} is not parsed as a URL")
        #print(f"An unexpected error occurred: {e}")
        logger.error(f"{rawURL} is not parsed as a URL")
        logger.error(f"An unexpected error occurred: {e}")

    #custom data cleaning for observed errors in IOSCO URL data
    
    if tidyURL.startswith("ttps://"):
        tidyURL = "h" + tidyURL
    if tidyURL.startswith("www.https://"):
        tidyURL = tidyURL[4:]
    if tidyURL.startswith("https://."):
        tidyURL = "https://" + tidyURL[9:]
    if tidyURL.startswith("htttps://"):
        tidyURL = "https://" + tidyURL[9:]
    if tidyURL.startswith("httops://"):
        tidyURL = "https://" + tidyURL[9:]
    if tidyURL.startswith("htpps://"):
        tidyURL = "https://" + tidyURL[8:]
    if tidyURL.startswith("https://www.."):
        tidyURL = "https://www." + tidyURL[13:]  
    if tidyURL.startswith("pagehttps://"):   
        tidyURL = tidyURL[4:]
    if tidyURL.startswith("pageshttps://"):   
        tidyURL = tidyURL[5:]
    if tidyURL.startswith("websitehttps://"):   
        tidyURL = tidyURL[7:]
    if tidyURL.startswith("websiteshttps://"):   
        tidyURL = tidyURL[8:]
    if tidyURL.startswith("andhttps://"):   
        tidyURL = tidyURL[3:]   

    return tidyURL


URLTuples_df[TIDY_URL] = URLTuples_df[rawURL].apply(tidyRawURL)

# manual data cleaning fixes - caused by data entry errors and limitations of the urlextract library
# id, src col, url, tidyurl

new_tuple = ('30231','additional_information','websiteswww.gmtdirect.com,www.gmtplatform.com', "https://www.gmtdirect.com") 

URLTuples_df.loc[len(URLTuples_df)] = new_tuple

new_tuple = ('30231','additional_information','websiteswww.gmtdirect.com,www.gmtplatform.com', "https://www.gmtplatform.com") 

URLTuples_df.loc[len(URLTuples_df)] = new_tuple

new_tuple = ('28662', 'additional_information', 'panel.billionaire-trade.co.comandhttps://trading.billionaire-trade.co.com', 'https://panel.billionaire-trade.co.com')

URLTuples_df.loc[len(URLTuples_df)] = new_tuple

new_tuple = ('28662', 'additional_information', 'panel.billionaire-trade.co.comandhttps://trading.billionaire-trade.co.com', 'https://trading.billionaire-trade.co.com')

URLTuples_df.loc[len(URLTuples_df)] = new_tuple

new_tuple = ('12828', 'additional_information', 'secure.capitalgmafx.comhttps://trade.capitalgmafx.com', 'https://secure.capitalgmafx.com')

URLTuples_df.loc[len(URLTuples_df)] = new_tuple

new_tuple = ('12828', 'additional_information', 'secure.capitalgmafx.comhttps://trade.capitalgmafx.com', 'https://trade.capitalgmafx.com')

URLTuples_df.loc[len(URLTuples_df)] = new_tuple

new_tuple = ('12828', 'additional_information', 'www.marketscfds.comhttps://secure.marketscfds.com', 'https://www.marketscfds.com')

URLTuples_df.loc[len(URLTuples_df)] = new_tuple

new_tuple = ('12828', 'additional_information', 'www.marketscfds.comhttps://secure.marketscfds.com', 'https://secure.marketscfds.com')

URLTuples_df.loc[len(URLTuples_df)] = new_tuple

new_tuple = ('12828', 'additional_information', 'ztrade24.comhttps://secure.ztrade24.com', 'https://ztrade24.com')

URLTuples_df.loc[len(URLTuples_df)] = new_tuple

new_tuple = ('12828', 'additional_information', 'ztrade24.comhttps://secure.ztrade24.com', 'https://secure.ztrade24.com')

URLTuples_df.loc[len(URLTuples_df)] = new_tuple

manual_data_cleaning_tuples_added = 10
logger.info(f"Number of domains added through manual data cleaning: {manual_data_cleaning_tuples_added}")

#print(f"Number of domains after manual data cleaning: {len(URLTuples_df)}"))
logger.info(f"Number of domains after manual data cleaning: {len(URLTuples_df)}")

#remove any URLs which are actually the domains of the regulators

logger.info("Checking for regulator URLs in list of dangerous domains...")

#list of IDs where the 'dangerous domain' is actually a regulator's domain
# which has been placed in the wrong field
regulatorIDsList = []
sourceColumnList = []
for regulatorDomain in regulatorDomainsList:
    for row in URLTuples_df.itertuples():
        url = getattr(row, TIDY_URL)
        if regulatorDomain in url:
            id = getattr(row, IOSCO_ID)
            sourceCol = getattr(row, SOURCE_COLUMN)
            regulatorIDsList.append(id)
            sourceColumnList.append(sourceCol)
            #print(f"{id} Regulator domain found: {url}")

logger.info(f"Number of instances of regulator domains found in URLs: {len(regulatorIDsList)}")
sourceColumnFreq = collections.Counter(sourceColumnList)
logger.info("Source columns of regulator URLs instances")
logger.info(sourceColumnFreq)

#remove the rows with instances of faulty regulator URLs

uniqueRegulatorIDsList = list(dict.fromkeys(regulatorIDsList))
logger.info(f"Number of unique IOSCO report IDs found with regulator domains in the extracted URL list {len(uniqueRegulatorIDsList)}")
logger.info(f"Number of extracted domain instances: {len(URLTuples_df)}")

filteredURLTuples_df = URLTuples_df[~URLTuples_df[IOSCO_ID].isin(uniqueRegulatorIDsList)]

logger.info(f"Number of domain instances removed as using regulator domains: {len(URLTuples_df) - len(filteredURLTuples_df)}")
logger.info(f"Number of extracted domain instances after regulator filtering: {len(filteredURLTuples_df)}")

urlList = list(filteredURLTuples_df[TIDY_URL].dropna())
uniqueURLList = list(dict.fromkeys(urlList))

logger.info(f"Number of domains: {len(urlList)}")
logger.info(f"Number of unique domains: {len(uniqueURLList)}")

#do we need to normalise these patterns?
#https://ztrade24.com
#https://secure.ztrade24.com
#No, because the regulator has listed them separately

resultsTuplesFileName = newDirName + '-results-tuples.csv'
resultsTuplesPath = Path(newDirPath,resultsTuplesFileName)

filteredResultsTuplesFileName = newDirName + '-results-tuples-filtered.csv'
filteredResultsTuplesPath = Path(newDirPath,resultsTuplesFileName)

URLTuples_df.to_csv(resultsTuplesPath, index=False, encoding='utf-8')
filteredURLTuples_df.to_csv(filteredResultsTuplesPath, index=False, encoding='utf-8')

#IOSCO_ID,source_column,rawURL,tidyURL

#summary stats for each daily file, as a list of tuples for saving to CSV file
resultsFileTuplesList = []

CSVCount = todaysCSV_df.count() # For each column the number of non-NA/null entries
# https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.count.html
for index, value in CSVCount.items():
    resultsFileTuplesList.append(( index, value ) )

resultsFileTuplesList.append( ("Filename", ioscoCSVPath) )
resultsFileTuplesList.append( ("Number_of_entries_in_CSV",len(todaysCSV_df) ) )
numNonEmptyurlRows = len(todaysCSV_df[urlCol].dropna())
resultsFileTuplesList.append( ("num_non-empty_rows_in_url_column", numNonEmptyurlRows ))
resultsFileTuplesList.append( ("url_coverage_percent", round(numNonEmptyurlRows / len(todaysCSV_df), 3)))
resultsFileTuplesList.append( ("num_domain_instances", len(urlList) ))
resultsFileTuplesList.append( ("num_unique_domain_instances", len(uniqueURLList) ))

dailyResultsFile_df = pd.DataFrame(resultsFileTuplesList, columns=["result", "value"])
dailyResultsFileName = newDirName + '-daily-results.csv'
dailyResultsFilePath = Path(newDirPath,dailyResultsFileName)
dailyResultsFile_df.to_csv(dailyResultsFilePath, index=False, encoding='utf-8')

#FB-specific processing
#filteredURLTuples_df
# numExtractedURLInstances = len(listofFoundURLTuples)
# print(f"Number of extracted URL instances: {numExtractedURLInstances}")

rawURLInstancesList = URLTuples_df[RAW_URL]
uniqueRawURLInstancesList = list(dict.fromkeys(rawURLInstancesList))

#custom FaceBook instances
fbCount = fbProfileCount = fbReelCount = fbAdsCount = fbPhotoCount = fbVideosCount = fbShareCount = 0

for item in uniqueRawURLInstancesList:
    if item.startswith("https://www.facebook.com"):
        fbCount += 1
        if '/videos/' in item:
            fbVideosCount += 1
    if item.startswith("https://www.facebook.com/profile"):
        fbProfileCount += 1
    if item.startswith("https://www.facebook.com/reel"):
        fbReelCount += 1
    if item.startswith("https://www.facebook.com/ads"):
        fbAdsCount += 1
    if item.startswith("https://www.facebook.com/photo"):
        fbPhotoCount += 1
    if item.startswith("https://www.facebook.com/share"):
        fbShareCount += 1
        
logger.info(f"Number of unique URL instances starting with https://www.facebook.com {fbCount}")
logger.info(f"Number of unique URL instances starting with https://www.facebook.com/profile {fbProfileCount}")
logger.info(f"Number of unique URL instances starting with https://www.facebook.com/ads {fbAdsCount}")
logger.info(f"Number of unique URL instances starting with https://www.facebook.com/share {fbShareCount}")
logger.info(f"Number of unique URL instances starting with https://www.facebook.com/reel {fbReelCount}")
logger.info(f"Number of unique URL instances starting with https://www.facebook.com/photo {fbPhotoCount}")
logger.info(f"Number of FB unique URL instances containing /videos/ {fbVideosCount}")

################## end of FB-specific processing

# send a list of URLs to Google Safe Browsing API
#def safeBrowsing(URLList: list) -> dict:
def safeBrowsing(tupleList: list) -> dict:
    #print(len(tupleList))
    URLList = []
    for tupleToCheck in tupleList:
        urlToCheck = tupleToCheck[3] # get the tidyURL
        URLList.append(urlToCheck)
    
    uniqueURLList = list(set(URLList))
    threatEntries = [{"url": url} for url in uniqueURLList]
    
    headers = {"Content-type": "application/json"}
    request_body = {
        "client": {
            "clientId": "ScamURLTester", 
            "clientVersion": "0.1"
        },
        "threatInfo": {
            "threatTypes": [ 
                "THREAT_TYPE_UNSPECIFIED",
                "MALWARE",
                "SOCIAL_ENGINEERING",
                "UNWANTED_SOFTWARE", 
                "POTENTIALLY_HARMFUL_APPLICATION"
            ],
            "platformTypes": ["ANY_PLATFORM"],
            "threatEntryTypes": ["URL"],
            "threatEntries": threatEntries
        }
    }
    try:
        response = requests.post(API_URL, headers=headers, data=json.dumps(request_body))
        # print(response.status_code)
        response.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)
        response_data = response.json()
        return response_data

    except requests.exceptions.HTTPError as err_h:
        logger.error(f"HTTP Status Code: {err_h.response.status_code}")
        logger.error(f"HTTP Error: {err_h.response.text}")
        return {'error': err_h.response.status_code}
    except requests.exceptions.ConnectionError as err_c:
        logger.error(f"Error Connecting: {err_c}")
    except requests.exceptions.Timeout as err_t:
        logger.error(f"Timeout Error: {err_t}")
    except requests.exceptions.RequestException as err:
        logger.error(f"Something went wrong: {err}")
    
    return {} # Return empty on error


# process the Dictionary structures returned by the Google API
#def safeBrowsingDict(urlsToCheck:  list, resultDict: dict, nowUTCString: str) -> list:
def safeBrowsingDict(urlstoCheckTupleList:  list, resultDict: dict, nowUTCString: str) -> list:
    #take the Google response Dict and output a list
    responseList = []
    for IOSCO_id, sourceColumn, rawURL, url in urlstoCheckTupleList:
        #print(f"length of responseList: {len(responseList)}")
        if MATCHES in resultDict: #are there any threat matches from Google at all
            #matchesList = resultDict[MATCHES]
            for match in resultDict[MATCHES]: #look at each item in matches list
                if match.get(THREAT): # is there a threat entry
                    threatURL = match.get(THREAT).get(URL).strip()
                    #print(f"threatURL is {threatURL}")
                    #print(match)
                    if url == threatURL: #is the url in threat entry in our tuple
                        threatResult = {
                            'time': nowUTCString,
                            IOSCO_ID: IOSCO_id,
                            SOURCE_COLUMN: sourceColumn,
                            RAW_URL: rawURL,
                            THREAT_TYPE : match.get(THREAT_TYPE),
                            PLATFORM_TYPE : match.get(PLATFORM_TYPE),
                            THREAT_ENTRY_TYPE : match.get(THREAT_ENTRY_TYPE),
                            URL : url,
                            THREAT_ENTRY_METADATA: match.get(THREAT_ENTRY_METADATA),
                            CACHE_DURATION : match.get(CACHE_DURATION)
                        }
                        #print(match.get(THREAT_ENTRY_METADATA))
                        responseList.append(threatResult)
                        #print(f"threatResult added: {threatResult}")
                        #print(f"length of responseList: {len(responseList)}")
                    else:
                        safeResult = {
                            'time': nowUTCString,
                            IOSCO_ID: IOSCO_id,
                            SOURCE_COLUMN: sourceColumn,
                            RAW_URL: rawURL,
                            THREAT_TYPE : '',
                            PLATFORM_TYPE : '',
                            THREAT_ENTRY_TYPE : '',
                            URL : url,
                            THREAT_ENTRY_METADATA: '',
                            CACHE_DURATION : ''
                        }
                        responseList.append(safeResult)
                    break #stop looking at this tuple
                    pass
        else:
            #all urls are not malicious
            safeResult = {
                            'time': nowUTCString,
                            IOSCO_ID: IOSCO_id,
                            SOURCE_COLUMN: sourceColumn,
                            RAW_URL: rawURL,
                            THREAT_TYPE : '',
                            PLATFORM_TYPE : '',
                            THREAT_ENTRY_TYPE : '',
                            URL : url,
                            THREAT_ENTRY_METADATA: '',
                            CACHE_DURATION : ''
            }
            responseList.append(safeResult)
                        
    return responseList

logger.info("Preparing to test URLs with Safe Browsing API...")

#query Google Safe Browsing API
#safeBrowsing = SafeBrowsing(GOOGLE_API_KEY)

URLTuples_df_test = filteredURLTuples_df
resultList = []
#print(URLTuples_df_test.head(10))
logger.info(URLTuples_df_test.head(10))
URLcount = 0
#URLsToTest = URLTuples_df_test[TIDY_URL]

failedValidationList = []

logger.info("Process batched tuple URLs with Google Safe Browsing API...")
#print(f"processing {len(URLTuples_df_test)} URLs")

logger.info(f"processing {len(URLTuples_df_test)} URLs")

#Request Size for threatMatches.find:
#When checking URLs against Safe Browsing lists using the threatMatches.find method, the #HTTP POST request can include up to 500 URLs.

for batchRows in itertools.batched(URLTuples_df_test.itertuples(), 500):
    tupleList = []
    itemList = list(batchRows)
    for item in itemList:
        ioscoid = item[1]
        sourceColumn = item[2]
        rawUrl = item[3]
        tidyUrl = item[4]
        tupleList.append((ioscoid, sourceColumn, rawUrl, tidyUrl))
    #print(tupleList)
    URLcount += len(tupleList)
    #print(f"URLcount = {URLcount}", end = "...")
    
    tuplesToCheck = []
    for tupleToCheck in tupleList:
        urlToCheck = tupleToCheck[3] # get the tidyURL
        #this 'url' should be a CONSTANT
        if any(d.get('url') == urlToCheck for d in resultList):
            #print(f"{urlToCheck} already checked")
            pass
        else:
            if validators.url(urlToCheck):
                tuplesToCheck.append(tupleToCheck)
            else:
                #print(f"{urlToCheck} failed URL validation")
                failedValidationList.append(tupleToCheck)
    try:
        nowUTCString = str(datetime.now(timezone.utc))

        #print(f"Length of tuplesToCheck: {len(tuplesToCheck)}") 

        resultDict = safeBrowsing(tuplesToCheck)

        #print(f"Length of resultDict: {len(resultDict)}") 
        
        #dict(list(resultDict.items())[:3])
        #[print(v) for i, v in enumerate(resultDict.items()) if i < 5]
        #print(resultDict)
        
        matches = resultDict.get('matches')
        #print(matches)
        #print(f"len of matches from resultDict: {len(matches)}")
        
        #id, sourcecol, url, tidyurl
        #urlstoCheckTupleList = [('123456','sourcecol', 'rawurl', url) for url in urlsToCheck]
        #print(urlstoCheckTupleList[0:5])
        
        dictResponseList = safeBrowsingDict(tuplesToCheck, resultDict, nowUTCString)
        #print(f"Length of dictResponseList: {len(dictResponseList)}") 
        
        resultList.extend(dictResponseList)

    except Exception as e:
        #print(f"Exception on URL {url}: {e}")
        #print(repr(e))
        #print(type(e))
        logger.error(f"Exception on URL {url}: {e}")
        logger.error(repr(e))
        logger.error(type(e))
        logger.error(traceback.print_exc())
        #nowUTCString = str(datetime.now(timezone.utc))           
               

logger.info(f"{URLcount} tuple URLs checked")
logger.info(f"Length of resultList: {len(resultList)}")
logger.info(f"{len(failedValidationList)} URLs failed to validate")

failValidationFileName = newDirName + "-failed-validation.txt"
failValidationPath = Path(newDirPath, failValidationFileName)

with open(failValidationPath, "w") as file:
    for failTuple in failedValidationList:
        file.write(f"{failTuple}\n")

#print(resultList)

#print(f"\n{URLcount} URLs checked")

# note only one entry for each URL
# need to post-process to produce tidy data


def copyResults(safeBrowsingResults_df):
    #if there is one entry for a url, copy results to all instances of URL
    processedURLsList = []
    for row in safeBrowsingResults_df.itertuples():
        if getattr(row, THREAT_ENTRY_TYPE):
            url = getattr(row, URL)
            if not url in processedURLsList:
                #rowsSameURL_df = safeBrowsingResults_df.loc[safeBrowsingResults_df[URL] == url]
                #print("shape of rowsSameURL", rowsSameURL_df.shape, " ", url)
                #rowsSameURL_df[THREAT_ENTRY_TYPE] = getattr(row, THREAT_ENTRY_TYPE)
                
                safeBrowsingResults_df.loc[safeBrowsingResults_df[URL] == url,THREAT_TYPE] = getattr(row, THREAT_TYPE)
                
                safeBrowsingResults_df.loc[safeBrowsingResults_df[URL] == url,THREAT_ENTRY_TYPE] = getattr(row, THREAT_ENTRY_TYPE)
                
                safeBrowsingResults_df.loc[safeBrowsingResults_df[URL] == url,PLATFORM_TYPE] = getattr(row, PLATFORM_TYPE)

                safeBrowsingResults_df.loc[safeBrowsingResults_df[URL] == url,THREAT_ENTRY_METADATA] = json.dumps(getattr(row, THREAT_ENTRY_METADATA))
                
                safeBrowsingResults_df.loc[safeBrowsingResults_df[URL] == url,CACHE_DURATION] = getattr(row, CACHE_DURATION)

                processedURLsList.append(url)

            else:
                   #print(f"{url} is already in processed list")
                   pass
        else:
            #print(f"No {THREAT_ENTRY_TYPE} for {getattr(row, URL)}")
            pass
    return safeBrowsingResults_df


safeBrowsingResults_df = pd.DataFrame(resultList)
tidySafeBrowsingResults_df = copyResults(safeBrowsingResults_df)

safeBrowsingFilename = newDirName + '-safe-browsing-results.csv'
safeBrowsingPath = Path(newDirPath, safeBrowsingFilename)
safeBrowsingResults_df.to_csv(safeBrowsingPath, encoding='utf-8')

tidySafeBrowsingFilename = newDirName + '-safe-browsing-results-tidy.csv'
tidySafeBrowsingPath = Path(newDirPath, tidySafeBrowsingFilename)
tidySafeBrowsingResults_df.to_csv(tidySafeBrowsingPath, encoding='utf-8')

logger.info(f"Length of safeBrowsingResults_df: {len(safeBrowsingResults_df)}")
logger.info(f"Length of tidySafeBrowsingResults_df: {len(tidySafeBrowsingResults_df)}")

logging.shutdown()
sys.exit()

###############################################################################

