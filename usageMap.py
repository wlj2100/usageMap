import re
import pandas as pd
import json
import numpy as np
# import operator
import time
import datetime

def parse(verbose=False):
    colHeads =  ['sessionid','cookieid','session_pid','timestamp','eaccountuserseq','eaccountuseremailaddressseq','cmusertrackingkey','contactseq','customerseq','marketingsiteseq','marketingentityseq','label','total_num_pages','total_session_time','first_page_contentcategory','first_page_category','first_page_name','first_page_url','first_page_duration','first_relevant_EDC_viewed','first_relevant_PGM_viewed','first_relevant_Class_viewed','first_relevant_Type_viewed','last_page_contentcategory','last_page_category','last_page_name','last_page_url','web_EDC_list','web_PGM_list','web_Class_list','web_type_list','line_event','session_date_time']

    line_event_schema = ['flag','index','date_time','event_duration','pagename','pageid','contentcategory','contentcategoryid','contentcategorytop','pageurl','searchresultscount','attribute7','itemcode','itemseq','branddescription','itemtypecode','itemclasscode','itemgroupmajorcode','manufacturercode','elementname','elementcategory','horiz_campaign','horiz_theme','sw_mgmt_campaign']
    # read, extract, generate and compare
    data  = []
    with open('liangji_dot_com_500_custD.txt','r') as f:
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
                # print line_event
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


'''
dataMap = {customerseq: {contactseq: {sessionid: record{key: value}}}}

record keys:
['timestamp','label','total_num_pages','total_session_time','web_EDC_list','web_PGM_list','web_type_list','web_Class_list','line_events']

line_event keys:
['flag','index','date_time','event_duration','pagename','pageid','contentcategory','contentcategoryid','contentcategorytop','pageurl','searchresultscount','attribute7','itemcode','itemseq','branddescription','itemtypecode','itemclasscode','itemgroupmajorcode','manufacturercode','elementname','elementcategory','horiz_campaign','horiz_theme','sw_mgmt_campaign']

companyMap = {customerseq: usage}}
accountMap = {customerseq: {contactseq: usage}}
usage = {}

'''

class UsageMap():
    """docstring for usageMap."""
    dataMap = {}
    customerMap = {}
    contactMap = {}

    def __init__(self):
        pass


    def getDataMap(self, data, oldMap=None, startDate=None, endDate=None, verbose=False):
        recordKeys = ['timestamp','label','total_num_pages','total_session_time','web_EDC_list','web_PGM_list','web_type_list','web_Class_list','line_events']

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
        # print 'company', len(usageMap)
        # print 'account', sum([len(usageMap[company]) for company in usageMap])
        # # print [key for key in usageMap]
        # print 'seesion', sum([len(usageMap[company][account]) for company in usageMap for account in usageMap[company]])
        # print 'data record', len(data)
        # # not match due to duplicate session
        # print 'duplicate', len(duplicate)
        # # print usageMap.keys()
        # print len(usageMap.keys())
        # # 20330908
        # print len(usageMap['20330908'])
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

if __name__ == '__main__':
    data = parse()
    obj = UsageMap()
    obj.getDataMap(data)
    obj.initUsageMap()
    obj.dataMap.keys()[0]
    obj.getCustomerUsage(obj.dataMap.keys()[0])
    print np.sum([item for item in obj.customerMap[obj.dataMap.keys()[0]]['class_focus']])
    obj.contactMap['20330908']['12385906']
    # sorted(obj.contactMap['20330908']['12385906']['class_focus'].items(), key=lambda x: x[1], reverse=True)



# def extend(usageMap):
#     for company in usageMap:
#         for account in usageMap[company]:
#             sessionDict = usageMap[company][account]
#             # get session count, class exist count
#             for



# class Customer():
#     """docstring for Company."""
#     contacts = {}
#     companyDetail = {}
#     def __init__(self):
#         pass
#
# class Contact:
#     sessions = {}
#     accountDetail = {}
#     def __init__(self):
#         pass
#
# def parseDataToClass(data):
#     recordKeys = ['timestamp','label','total_num_pages','total_session_time','web_EDC_list','web_PGM_list','web_type_list','web_Class_list','line_events']
#     data = parse()
#     usageMap = {}
#     duplicate = []
#     for item in data:
#         if item['customerseq'] not in usageMap:
#             usageMap[item['customerseq']] = Customer()
#         # type(usageMap[item['customerseq']].contacts)
#         if item['contactseq'] not in usageMap[item['customerseq']].contacts:
#             usageMap[item['customerseq']].contacts[item['contactseq']] = Contact()
#         # type(usageMap[item['customerseq']].contacts[item['contactseq']].sessions)
#         record = {}
#         for key in recordKeys:
#             record[key] = item[key]
#         if item['sessionid'] in usageMap[item['customerseq']].contacts[item['contactseq']].sessions:
#             print 'warning: duplicate session id at %s...' %(item['sessionid'][:10])
#             duplicate.append(item)
#         elif item['sessionid'] != '' and item['sessionid'] not in usageMap[item['customerseq']].contacts[item['contactseq']].sessions:
#             usageMap[item['customerseq']].contacts[item['contactseq']].sessions[item['sessionid']] = record
#         else:
#             duplicate.append(item)
#     print 'company', len(usageMap)
#     print 'account', sum([len(usageMap[company].contacts) for company in usageMap])
#     # print [key for key in usageMap]
#     print 'seesion', sum([len(usageMap[company].contacts[account].sessions) for company in usageMap for account in usageMap[company].contacts])
#     print 'data record', len(data)
#     # not match due to duplicate session
#     print 'duplicate', len(duplicate)
#     # print usageMap.keys()
#     print len(usageMap.keys())
#     # 20330908
#     print len(usageMap['20330908'].contacts)
#     return usageMap
