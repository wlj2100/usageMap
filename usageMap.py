import pandas as pd
import json
import numpy as np
import time
import datetime
from collections import Counter
'''
dataMap = {customerseq: {contactseq: {sessionid: record{key: value}}}}

record keys:
['timestamp','label','total_num_pages','total_session_time','web_EDC_list','web_PGM_list','web_type_list','web_Class_list','line_events','city','state','country','num_product','num_search']

line_event keys:
['flag','index','date_time','event_duration','pagename','pageid','contentcategory','contentcategoryid','contentcategorytop','pageurl','searchresultscount','attribute7','itemcode','itemseq','branddescription','itemtypecode','itemclasscode','itemgroupmajorcode','manufacturercode','elementname','elementcategory','horiz_campaign','horiz_theme','sw_mgmt_campaign']

companyMap = {customerseq: usage}}
accountMap = {customerseq: {contactseq: usage}}
usage = {}

'''



class UsageMap():
    """docstring for usageMap."""
    # dataMap = {}
    # customerMap = {}
    # contactMap = {}

    def __init__(self):
        self.dataMap = {}
        self.customerMap = {}
        self.contactMap = {}


    def getDataMap(self, data, oldMap=None, startDate=None, endDate=None, verbose=False):
        recordKeys = ['timestamp','label','total_num_pages','total_session_time','web_EDC_list','web_PGM_list','web_type_list','web_Class_list','line_events','referralsource','city','state','country']

        dataMap = {}
        if oldMap != None:
            dataMap = oldMap

        duplicate = []
        for item in data:
            if item['customerseq'] not in dataMap:
                dataMap[item['customerseq']] = {}
            if item['contactseq'] not in dataMap[item['customerseq']]:
                dataMap[item['customerseq']][item['contactseq']] = {}
            record = {}
            for key in recordKeys:
                record[key] = item[key]
            if item['sessionid'] in dataMap[item['customerseq']][item['contactseq']]:
                # print 'warning: duplicate session id at %s...' %(item['sessionid'][:10])
                duplicate.append(item)
            elif item['sessionid'] != '' and item['sessionid'] not in dataMap[item['customerseq']][item['contactseq']]:
                # check date
                if startDate != None:
                    if timeConvert(record['timestamp']) < startDate:
                        continue
                if endDate != None:
                    if timeConvert(record['timestamp']) > endDate:
                        continue
                dataMap[item['customerseq']][item['contactseq']][item['sessionid']] = record
            else:
                duplicate.append(item)
        self.dataMap = dataMap

    def initUsageMap(self):
        # init data structure
        dataMap = self.dataMap
        customerMap = {}
        contactMap = {}
        for customer in dataMap:
            customerMap[customer] = {}
            contactMap[customer] = {}

            customerMap[customer]['class_focus'] = []
            for contact in dataMap[customer]:
                contactMap[customer][contact] = {}
                contactMap[customer][contact]['class_focus'] = []
                contactMap[customer][contact]['number_session'] = len(dataMap[customer][contact])

                validSession = 0
                for session in dataMap[customer][contact]:
                    record = dataMap[customer][contact][session]
                    # for contact level:
                    contactMap[customer][contact]['class_focus'] += record['web_Class_list']
                    # for customer level: remove dup by list(set())
                    customerMap[customer]['class_focus'] += list(set(record['web_Class_list']))
                    if len(record['web_Class_list']) > 0:
                        validSession += 1
                contactMap[customer][contact]['contact_type'] = self.accountUsage(validSession, len(dataMap[customer][contact]))
                contactMap[customer][contact]['class_focus'] = self.get_focus(contactMap[customer][contact]['class_focus'])


                # ITEMS = ['label','total_num_pages','total_session_time','web_EDC_list','web_PGM_list','web_type_list','web_Class_list','referralsource','city','state','country']
                # for numeric attribute
                NUM_ITEMS = ['label',
                'total_num_pages',
                'total_session_time',
                'web_EDC_list','web_PGM_list','web_type_list','web_Class_list']
                temps = self.contactNumAnalysis(dataMap[customer][contact], NUM_ITEMS)
                for i in range(len(NUM_ITEMS)):
                    contactMap[customer][contact][NUM_ITEMS[i] + '_Analysis'] = temps[i]

                # for string attribute
                STR_ITEMS = ['web_EDC_list','web_PGM_list','web_type_list','web_Class_list','referralsource','city','state','country']
                temps = self.contactStrAnalysis(dataMap[customer][contact], STR_ITEMS)
                for i in range(len(STR_ITEMS)):
                    contactMap[customer][contact][STR_ITEMS[i] + '_Analysis'] = temps[i]
            # temp = {}
            # countSum = len(customerMap[customer]['class_focus'])
            # for cls in customerMap[customer]['class_focus']:
            #     if cls not in temp:
            #         temp[cls] = 0
            #     temp[cls] += 1
            # for key in temp:
            #     temp[key] = (temp[key], self.classFocus(temp[key], countSum))
            customerMap[customer]['class_focus'] = self.get_focus(customerMap[customer]['class_focus'])
        self.customerMap = customerMap
        self.contactMap = contactMap

    def contactNumAnalysis(self, dataMapDict, ITEMS, bins='auto', density=False):
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

    def contactStrAnalysis(self, dataMapDict, ITEMS):
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

    def get_focus(self, lst):
        temp = {}
        countSum = len(lst)
        for cls in lst:
            if cls not in temp:
                temp[cls] = 0
            temp[cls] += 1
        for key in temp:
            temp[key] = (temp[key], self.classFocus(temp[key], countSum))

        return sorted(temp.items(), key=lambda x: x[1], reverse=True)


    def getCustomerUsage(self, customerseq):
        if customerseq in self.customerMap:
            return self.customerMap[customerseq]
        else:
            print 'customerseq not exist'

    def getContactUsage(self, customerseq, contactseq):
        if customerseq in self.contactMap:
            if contactseq in self.contactMap[customerseq]:
                return self.contactMap[customerseq][contactseq]
            else:
                print 'contactseq not exist'
        else:
            print 'customerseq not exist'

    def accountUsage(self, validCount, sessionCount):
        if sessionCount < 3:
            return 'ignore'
        if validCount / (sessionCount + 0.0) <= 0.1:
            return 'manage'
        if validCount / (sessionCount + 0.0) >= 0.6:
            return 'look'
        return 'mix'

    def classFocus(self, count, countSum):
        if count < 5:
            return 'ignore'
        if count / (countSum + 0.0) >= .5:
            return 'focus'
        if countSum > 25 and count / (countSum + 0.0) >= .4:
            return 'focus'
        if countSum > 50 and count / (countSum + 0.0) >= .25:
            return 'focus'
        if countSum > 200 and count / (countSum + 0.0) >= .2:
            return 'focus'
        return 'ignore'

    def getCustomerMap(self):
        return self.customerMap

    def getContactMap(self):
        return self.contactMap
