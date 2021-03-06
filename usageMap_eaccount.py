import pandas as pd
import json
import numpy as np
import time
import datetime
from collections import Counter
'''
dataMap = {customerseq: {eaccountseq: {sessionid: record{key: value}}}}

record keys:
['timestamp','label','total_num_pages','total_session_time','web_EDC_list','web_PGM_list','web_type_list','web_Class_list','line_events','city','state','country','num_product','num_search']

line_event keys:
['flag','index','date_time','event_duration','pagename','pageid','contentcategory','contentcategoryid','contentcategorytop','pageurl','searchresultscount','attribute7','itemcode','itemseq','branddescription','itemtypecode','itemclasscode','itemgroupmajorcode','manufacturercode','elementname','elementcategory','horiz_campaign','horiz_theme','sw_mgmt_campaign']

companyMap = {customerseq: usage}}
accountMap = {customerseq: {eaccountseq: usage}}
usage = {}

'''



class UsageMap():
    """docstring for usageMap."""

    def __init__(self):
        self.dataMap = {}
        self.eaccountMap = {}


    def getDataMap(self, data, oldMap=None, startDate=None, endDate=None, verbose=False):
        recordKeys = ['timestamp','label','total_num_pages','total_session_time','web_EDC_list','web_PGM_list','web_type_list','web_Class_list','line_events','referralsource','city','state','country','dma','eaccountuserseq','contactseq']

        dataMap = {}
        if oldMap != None:
            dataMap = oldMap

        duplicate = []
        for item in data:
            if item['customerseq'] not in dataMap:
                dataMap[item['customerseq']] = {}
            if item['eaccountuserseq'] not in dataMap[item['customerseq']]:
                dataMap[item['customerseq']][item['eaccountuserseq']] = {}
            '''
            record = {}
            for key in recordKeys:
                record[key] = item[key]
            '''
            record = item
            if item['sessionid'] in dataMap[item['customerseq']][item['eaccountuserseq']]:
                print 'warning: duplicate session id at %s...' %(item['sessionid'][:10])
                duplicate.append(item)
            elif item['sessionid'] != '' and item['sessionid'] not in dataMap[item['customerseq']][item['eaccountuserseq']]:
                # check date
                if startDate != None:
                    if timeConvert(record['timestamp']) < startDate:
                        continue
                if endDate != None:
                    if timeConvert(record['timestamp']) > endDate:
                        continue
                dataMap[item['customerseq']][item['eaccountuserseq']][item['sessionid']] = record
            else:
                duplicate.append(item)
        self.dataMap = dataMap

    def initUsageMap(self, startDate=None):
        # init data structure
        dataMap = self.dataMap
        eaccountMap = {}
        # init eaccountMap
        for customer in dataMap:
            eaccountMap[customer] = {}
            # get eaccount
            for eaccount in dataMap[customer]:
                eaccountMap[customer][eaccount] = {}
                eaccountMap[customer][eaccount]['number_session'] = len(dataMap[customer][eaccount])

                for session in dataMap[customer][eaccount]:
                    record = dataMap[customer][eaccount][session]

                # ITEMS = ['label','total_num_pages','total_session_time','web_EDC_list','web_PGM_list','web_type_list','web_Class_list','referralsource','city','state','country']
                # for numeric attribute
                NUM_ITEMS = ['label',
                'total_num_pages',
                'total_session_time',
                'web_EDC_list','web_PGM_list','web_type_list','web_Class_list']
                temps = self.eaccountNumAnalysis(dataMap[customer][eaccount], NUM_ITEMS)
                for i in range(len(NUM_ITEMS)):
                    eaccountMap[customer][eaccount][NUM_ITEMS[i] + '_Num_Analysis'] = temps[i]

                # for string attribute
                STR_ITEMS = ['dma','web_EDC_list','web_PGM_list','web_type_list','web_Class_list','referralsource','city','state','country','contactseq']
                temps = self.eaccountStrAnalysis(dataMap[customer][eaccount], STR_ITEMS)
                for i in range(len(STR_ITEMS)):
                    eaccountMap[customer][eaccount][STR_ITEMS[i] + '_Str_Analysis'] = temps[i]

        self.eaccountMap = eaccountMap

    def eaccountNumAnalysis(self, dataMapDict, ITEMS, bins='auto', density=False):
        temps = []
        for i in range(len(ITEMS)):
            temps.append([])
        for session in dataMapDict:
            for i in range(len(ITEMS)):
                ITEM = ITEMS[i]
                if type(dataMapDict[session][ITEM]) is list:
                    # temps[i] += (dataMapDict[session][ITEM])
                    temps[i].append(len(dataMapDict[session][ITEM]))
                else:
                    if type(dataMapDict[session][ITEM]) is str:
                        if len(dataMapDict[session][ITEM]) is 0:
                            dataMapDict[session][ITEM] = 0
                        try:
                            dataMapDict[session][ITEM] = float(dataMapDict[session][ITEM])
                        except Exception as e:
                            print ITEM, dataMapDict[session][ITEM]
                            raise

                    temps[i].append(dataMapDict[session][ITEM])
        # temp = [dataMapDict[session][ITEM] for session in dataMapDict]

        for i in range(len(ITEMS)):
            try:
                hist, edges = np.histogram(temps[i], bins=bins, density=density)
                total = np.sum(hist)
                percentage = 0
                if total > 0:
                    percentage = hist.astype(float) / float(total)
                temps[i] = {
                'hist': hist,
                'percentage': percentage,
                'edges': edges,
                'total': total
                }
            except Exception as e:
                print ITEMS[i] + ' fails parse'
                print temps[i]
                temps[i] = {}

        # return np.histogram(temp, bin=5)
        return temps
        pass

    def eaccountStrAnalysis(self, dataMapDict, ITEMS):
        temps = []
        for i in range(len(ITEMS)):
            temps.append([])
        for session in dataMapDict:
            for i in range(len(ITEMS)):
                ITEM = ITEMS[i]
                if type(dataMapDict[session][ITEM]) is list:
                    temps[i] += (dataMapDict[session][ITEM])
                else:
                    temps[i].append(dataMapDict[session][ITEM])
        # temp = [dataMapDict[session][ITEM] for session in dataMapDict]

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
                'hist': hist,
                'percentage': percentage,
                'key': keys,
                'total': total
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
        pass

    def timeConvert(self, s):
        return str(int(time.mktime(datetime.datetime.strptime(s, "%d/%m/%Y").timetuple())))
