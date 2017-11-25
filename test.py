import re
import pandas as pd
import json
import numpy as np
# import operator
import time
import datetime
# from usageMap_eaccount import UsageMap
from collections import Counter
from pprint import pprint
from scipy import stats
import copy
def parse(verbose=False):
    colHeads = ['sessionid','cookieid','session_pid','firsttimestamp','firstreferringurl','firstdestinationurl','firstreferraltype','timestamp','referralname','referralurl','referraltype','referralsource','city','state','country','dma','secondleveldomain','browsertype','javascriptversion','language','screenresolution','operatingsystemname','mobilenetwork','mobiledevice','devicetype','eaccountuserseq','eaccountuseremailaddressseq','cmusertrackingkey','contactseq','customerseq','marketingsiteseq','marketingentityseq','label','total_num_pages','total_num_elements','num_product','num_search','num_shop','num_account_center','num_homepage','num_solutions_and_services','num_hubs','num_custom_platinum_pages','num_brands','num_other','num_pdf','num_video','num_other_media_library_digital_assets','total_session_time','first_page_contentcategory','first_page_category','first_page_name','first_page_url','first_page_duration','first_relevant_EDC_viewed','first_relevant_PGM_viewed','first_relevant_Class_viewed','first_relevant_Type_viewed','last_page_contentcategory','last_page_category','last_page_name','last_page_url','web_EDC_list','web_PGM_list','web_Class_list','web_type_list','line_event','session_date_time']


    line_event_schema = ['flag','index','date_time','event_duration','pagename','pageid','contentcategory','contentcategoryid','contentcategorytop','pageurl','searchresultscount','attribute7','itemcode','itemseq','branddescription','itemtypecode','itemclasscode','itemgroupmajorcode','manufacturercode','elementname','elementcategory','horiz_campaign','horiz_theme','sw_mgmt_campaign','page_referral_source','search_keyword','ip']
    # read, extract, generate and compare
    data  = []
    # with open('liangji_dot_com_500_custD.txt','r') as f:
    with open('./new_data/full_sample_for_liangji_500D','r') as f:
        count = 0
        for line in f:
            count += 1
            temp = {}
            contents = line.split('\t')
            # for content in contents:
            #     print content
            for i in range(len(colHeads)):
                temp[colHeads[i]] = contents[i].strip()
            # deal with line_event
            line_events = temp['line_event'][1: -1].split('),(')
            temp['line_events'] = []
            for i in range(len(line_events)):
                temp2 = {}
                line_event = line_events[i]
                # print line_events[i]
                if i == 0:
                    line_event = line_event[1:]
                if len(line_event) > 1 and i == len(line_events) - 1:
                    line_event = line_event[:-1]
                line_event = line_event.split(',')
                # print line_event
                # print len(line_event)
                # print len(line_event_schema)
                for j in range(len(line_event)):
                    # print line_event_schema[j], line_event[j]
                    temp2[line_event_schema[j]] = line_event[j]
                # print temp2
                temp['line_events'].append(temp2)
            # deal with web_xxxlist
            temp['web_EDC_list'] = [x for x in temp['web_EDC_list'].strip().split(',') if x != '']
            temp['web_EDC_amount'] = len(temp['web_EDC_list'])
            temp['web_PGM_list'] = [x for x in temp['web_PGM_list'].strip().split(',') if x != '']
            temp['web_PGM_amount'] = len(temp['web_PGM_list'])
            temp['web_Class_list'] = [x for x in temp['web_Class_list'].strip().split(',') if x != '']
            temp['web_Class_amount'] = len(temp['web_Class_list'])
            temp['web_type_list'] = [x for x in temp['web_type_list'].strip().split(',') if x != '']
            temp['web_type_amount'] = len(temp['web_type_list'])
            # print temp['web_PGM_list']

            data.append(temp)
            # if count > 10:
            #     break
    return data

def getDataMap(data, oldMap=None, startTimestamp=None, endTimestamp=None, verbose=False):
    # recordKeys = ['timestamp','label','total_num_pages','total_session_time','web_EDC_list','web_PGM_list','web_type_list','web_Class_list','line_events','referralsource','city','state','country','dma','eaccountuserseq','contactseq']
    dataMap = {}
    if oldMap != None:
        dataMap = oldMap
    timestampNullCount = 0
    duplicate = []
    for item in data:
        if item['customerseq'] not in dataMap:
            dataMap[item['customerseq']] = {}
        if item['eaccountuserseq'] not in dataMap[item['customerseq']]:
            dataMap[item['customerseq']][item['eaccountuserseq']] = {}

        # record = {}
        # for key in recordKeys:
        #     record[key] = item[key]

        record = item
        if len(item['timestamp']) != 10:
            # print item['timestamp']
            # item['timestamp'] = item['sessionid'].split('|')[-1]
            # print item['timestamp']
            timestampNullCount += 1
        if item['sessionid'] in dataMap[item['customerseq']][item['eaccountuserseq']]:
            # print 'warning: duplicate session id at %s...' %(item['sessionid'][:10])
            duplicate.append(item)
        elif item['sessionid'] != '' and item['sessionid'] not in dataMap[item['customerseq']][item['eaccountuserseq']]:
            # check date
            if startTimestamp != None:
                if timeConvert(record['timestamp']) < startTimestamp:
                    continue
            if endTimestamp != None:
                if timeConvert(record['timestamp']) > endTimestamp:
                    continue
            dataMap[item['customerseq']][item['eaccountuserseq']][item['sessionid']] = record
        else:
            duplicate.append(item)
    print 'duplicate session count: ', len(duplicate)
    print 'timestamp Null Count', timestampNullCount
    print 'new data len:', len(data)
    print 'dataMap len:', len(dataMap)
    with open('dataMap.json', 'w') as f:
        json.dump(dataMap, f)
    return dataMap

def getEaccountMap(jsonPath=None, dataMap=None, startTimestamp=None, endTimestamp=None):
    # init data structure
    if jsonPath is not None:
        with open(jsonPath, 'r') as json_data:
            dataMap = json.load(json_data)
    eaccountMap = {}
    if dataMap is None:
        return eaccountMap
    # init eaccountMap
    for customer in dataMap:
        eaccountMap[customer] = {}
        # get eaccount
        for eaccount in dataMap[customer]:
            eaccountMap[customer][eaccount] = {}

            sessionDict = dataMap[customer][eaccount]
            if startTimestamp != None:
                for session in sessionDict:
                    if sessionDict[session]['timestamp'] < startTimestamp:
                        del sessionDict[session]
            if endTimestamp != None:
                for session in sessionDict:
                    if sessionDict[session]['timestamp'] > endTimestamp:
                        del sessionDict[session]

            eaccountMap[customer][eaccount]['number_session'] = len(sessionDict)
            # ITEMS = ['label','total_num_pages','total_session_time','web_EDC_list','web_PGM_list','web_type_list','web_Class_list','referralsource','city','state','country']
            # for numeric attribute
            NUM_ITEMS = ['label','web_EDC_list','web_PGM_list','web_type_list','web_Class_list']
            temps = eaccountNumAnalysis(sessionDict, NUM_ITEMS)
            for i in range(len(NUM_ITEMS)):
                eaccountMap[customer][eaccount][NUM_ITEMS[i] + '_Num_Analysis'] = temps[i]

            # for string attribute
            STR_ITEMS = ['dma','web_EDC_list','web_PGM_list','web_type_list','web_Class_list','referralsource','city','state','country','contactseq', 'operatingsystemname']
            temps = eaccountStrAnalysis(sessionDict, STR_ITEMS)
            for i in range(len(STR_ITEMS)):
                eaccountMap[customer][eaccount][STR_ITEMS[i] + '_Str_Analysis'] = temps[i]

            # platform analysis
            eaccountMap[customer][eaccount]['platform'] = platformAnalysis(eaccountMap[customer][eaccount]['operatingsystemname_Str_Analysis'])
    return eaccountMap

def platformAnalysis(osCountDict):
    # for platform
    mobileList = ['Mobile','iOS','Android','Phone','Samsung','Nokia','RTM', 'Tizen', 'PlayStation','Symbian','Asha','Firefox','webOS']
    desktopList = ['Windows','OS X','Linux','Macintosh','SunOS','Media','MeeGo']
    mobileCount = 0
    desktopCount = 0
    temp = {}
    for i in range(len(osCountDict['key'])):
        if len([s for s in mobileList if s in osCountDict['key'][i]]) > 0:
            mobileCount += osCountDict['hist'][i]
        elif len([s for s in desktopList if s in osCountDict['key'][i]]) > 0:
             desktopCount += osCountDict['hist'][i]
    temp['key'] = ['mobile', 'desktop', 'other']
    temp['hist'] = [mobileCount, desktopCount, osCountDict['total'] - mobileCount - desktopCount]
    temp['percentage'] = [float(mobileCount)/osCountDict['total'], float(desktopCount)/osCountDict['total'], float(temp['hist'][2])/osCountDict['total']]
    temp['total'] = osCountDict['total']
    return temp

# take a session dictionary and perfom special analysis for fixed bins
def specialNumericAnalysis(sessionDict):
    # dict of each feature dict
    result = {}
    sessionTimeCounts = [0] * 6
    timeOfDayCounts = [0] * 6
    numOfPagesCounts = [0] * 6
    for session in sessionDict:
        sessionTimeHelper(sessionDict[session]['total_session_time'] / 60, sessionTimeCounts)
        timeOfDayHelper(sessionDict[session]['timestamp'], timeOfDayCounts)
        numOfPagesHelper(sessionDict[session]['total_num_pages'], numOfPagesCounts)
    result['sessionTimeAnalysis'] = sessionTimeAnalysis(sessionTimeCounts)
    result['timeOfDayAnalysis'] = timeOfDayAnalysis(timeOfDayCounts)
    result['numOfPagesAnalysis'] = numOfPagesAnalysis(numOfPagesCounts)
    return result
    pass

def numOfPagesHelper(num, count):
    if num < 2:
        count[0] += 1
    elif num < 4:
        count[1] += 1
    elif num < 8:
        count[2] += 1
    elif num < 16:
        count[3] += 1
    elif num < 31:
        count[4] += 1
    else:
        count[5] += 1

def numOfPagesAnalysis(numOfPagesCounts):
    numOfPagesDict = {}
    numOfPagesDict['edge'] = ['1','2-3','4-7','8-15','16-30','31+']
    numOfPagesDict['hist'] = numOfPagesCounts
    numOfPagesDict['total'] = sum(numOfPagesCounts)
    numOfPagesDict['percentage'] = (np.array(numOfPagesCounts) / float(numOfPagesDict['total'])).tolist()
    numOfPagesDict['entropy'] = stats.entropy(np.array(numOfPagesDict['percentage']))
    return numOfPagesDict

def sessionTimeHelper(minute, count):
    if minute < 2:
        count[0] += 1
    elif minute < 5:
        count[1] += 1
    elif minute < 9:
        count[2] += 1
    elif minute < 21:
        count[3] += 1
    elif minute < 46:
        count[4] += 1
    else:
        count[5] += 1

def sessionTimeAnalysis(sessionTimeCounts):
    sessionTimeDict = {}
    sessionTimeDict['edge'] = ['0-1min','2-4','5-8','9-20','21-45','46+']
    sessionTimeDict['hist'] = sessionTimeCounts
    sessionTimeDict['total'] = sum(sessionTimeCounts)
    sessionTimeDict['percentage'] = (np.array(sessionTimeCounts) / float(sessionTimeDict['total'])).tolist()
    sessionTimeDict['entropy'] = stats.entropy(np.array(sessionTimeDict['percentage']))
    return sessionTimeDict
# use same timezone right now
# need to change later
# stackoverflow.com/questions/16505501/get-timezone-from-city-in-python-django

def timeOfDayAnalysis(timeOfDayCounts):
    timeOfDayDict = {}
    timeOfDayDict['edge'] = ['0-7 EM','8-10 M','11-13 L','14-16 A','17-20 E','21-23 N']
    timeOfDayDict['hist'] = timeOfDayCounts
    timeOfDayDict['total'] = sum(timeOfDayCounts)
    timeOfDayDict['percentage'] = (np.array(timeOfDayCounts) / float(timeOfDayDict['total'])).tolist()
    timeOfDayDict['entropy'] = stats.entropy(np.array(timeOfDayDict['percentage']))
    return timeOfDayDict
    pass

def timeOfDayHelper(timestamp, count):
    time = getHourFromTimestampToChicago(timestamp)
    if time < 8:
        count[0] += 1
    elif time < 11:
        count[1] += 1
    elif time < 14:
        count[2] += 1
    elif time < 17:
        count[3] += 1
    elif time < 21:
        count[4] += 1
    else:
        count[5] += 1

def eaccountNumAnalysis(sessionDict, ITEMS, bins='auto', density=False):
    temps = []
    for i in range(len(ITEMS)):
        temps.append([])
    for session in sessionDict:
        for i in range(len(ITEMS)):
            ITEM = ITEMS[i]
            if type(sessionDict[session][ITEM]) is list:
                # temps[i] += (sessionDict[session][ITEM])
                temps[i].append(len(sessionDict[session][ITEM]))
            else:
                if type(sessionDict[session][ITEM]) is str:
                    if len(sessionDict[session][ITEM]) is 0:
                        sessionDict[session][ITEM] = 0
                    try:
                        sessionDict[session][ITEM] = float(sessionDict[session][ITEM])
                    except Exception as e:
                        print ITEM, sessionDict[session][ITEM]
                        raise

                temps[i].append(sessionDict[session][ITEM])
    # temp = [sessionDict[session][ITEM] for session in sessionDict]

    for i in range(len(ITEMS)):
        try:
            hist, edges = np.histogram(temps[i], bins=bins, density=density)
            total = np.sum(hist)
            percentage = 0
            if total > 0:
                percentage = hist.astype(float) / float(total)
            temps[i] = {
            'hist': hist.tolist(),
            'percentage': percentage.tolist(),
            'edges': edges.tolist(),
            'total': total,
            'entropy': stats.entropy(np.array(percentage))
            }
        except Exception as e:
            print ITEMS[i] + ' fails parse'
            print temps[i]
            temps[i] = {}

    # return np.histogram(temp, bin=5)
    return temps

def eaccountStrAnalysis(sessionDict, ITEMS):
    temps = []
    for i in range(len(ITEMS)):
        temps.append([])
    for session in sessionDict:
        for i in range(len(ITEMS)):
            ITEM = ITEMS[i]
            if type(sessionDict[session][ITEM]) is list:
                temps[i] += (sessionDict[session][ITEM])
            else:
                temps[i].append(sessionDict[session][ITEM])
    # temp = [sessionDict[session][ITEM] for session in sessionDict]

    for i in range(len(ITEMS)):
        try:
            temps[i] = Counter(temps[i])
            total = np.sum(temps[i].values())
            keys = temps[i].keys()
            hist = np.array(temps[i].values())
            percentage = 0
            # error here
            if total > 0:
                percentage = hist.astype(float) / float(total)
            temps[i] = {
            'hist': hist.tolist(),
            'percentage': percentage.tolist(),
            'key': keys,
            'total': total,
            'entropy': stats.entropy(np.array(percentage))
            }
        except Exception as e:
            print ITEMS[i] + ' fails parse'
            print temps[i]
            print percentage
            print hist
            print total
            temps[i] = {}
            raise

    # return np.histogram(temp, bin=5)
    return temps


def timeConvert(s):
    return str(int(time.mktime(datetime.datetime.strptime(s, "%d/%m/%Y").timetuple())))

def getHourFromTimestampToChicago(timestamp):
    obj = time.gmtime(float(timestamp))
    return (int(time.strftime("%H", obj)) - 6) % 24

if __name__ == '__main__':
    data = parse()
    dataMap = getDataMap(data)
    eaccountMap = getEaccountMap('dataMap.json', dataMap)
    dataMap.keys()[0]
    dataMap['20330908'].keys()[0]
    # getCustomerUsage(dataMap.keys()[0])
    # customerMap[dataMap.keys()[0]]['class_focus']
    # print np.sum([item[1][0] for item in customerMap[dataMap.keys()[0]]['class_focus']])
    pprint(eaccountMap['20330908']['7162605'])
    for key in dataMap['20330908']['7162605']:
        print key
        print dataMap['20330908']['7162605'][key]['timestamp']

    # for key in contactMap['20330908']['12385906']:
    #     print 'key:', key
    #     print contactMap['20330908']['12385906'][key]
    #     print ''
    # type(dataMap['20330908']['12385906'])
    # contactMap['20330908']['12385906']['label_Analysis']['edges']
    # dataMap['20330908']['12385906'].keys()
    pass
