import re
import pandas as pd
import json
import numpy as np
# import operator
import time
import datetime
from usageMap import UsageMap
from collections import Counter
def parse(verbose=False):


    colHeads = ['sessionid','cookieid','session_pid','firsttimestamp','firstreferringurl','firstdestinationurl','firstreferraltype','timestamp','referralname','referralurl','referraltype','referralsource','city','state','country','dma','secondleveldomain','browsertype','javascriptversion','language','screenresolution','operatingsystemname','mobilenetwork','mobiledevice','devicetype','eaccountuserseq','eaccountuseremailaddressseq','cmusertrackingkey','contactseq','customerseq','marketingsiteseq','marketingentityseq','label','total_num_pages','total_num_elements','num_product','num_search','num_shop','num_account_center','num_homepage','num_solutions_and_services','num_hubs','num_custom_platinum_pages','num_brands','num_other','num_pdf','num_video','num_other_media_library_digital_assets','total_session_time','first_page_contentcategory','first_page_category','first_page_name','first_page_url','first_page_duration','first_relevant_EDC_viewed','first_relevant_PGM_viewed','first_relevant_Class_viewed','first_relevant_Type_viewed','last_page_contentcategory','last_page_category','last_page_name','last_page_url','web_EDC_list','web_PGM_list','web_Class_list','web_type_list','line_event','session_date_time']


    line_event_schema = ['flag','index','date_time','event_duration','pagename','pageid','contentcategory','contentcategoryid','contentcategorytop','pageurl','searchresultscount','attribute7','itemcode','itemseq','branddescription','itemtypecode','itemclasscode','itemgroupmajorcode','manufacturercode','elementname','elementcategory','horiz_campaign','horiz_theme','sw_mgmt_campaign','page_referral_source','search_keyword','ip']
    # read, extract, generate and compare
    data  = []
    # with open('liangji_dot_com_500_custD.txt','r') as f:
    with open('./new_data/full_sample_for_liangji_500D.txt','r') as f:
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



if __name__ == '__main__':
    data = parse()
    obj = UsageMap()
    obj.getDataMap(data)
    obj.initUsageMap()
    # obj.dataMap.keys()[0]
    # obj.getCustomerUsage(obj.dataMap.keys()[0])
    # obj.customerMap[obj.dataMap.keys()[0]]['class_focus']
    # print np.sum([item[1][0] for item in obj.customerMap[obj.dataMap.keys()[0]]['class_focus']])

    for key in obj.contactMap['20330908']['12385906']:
        print key
        print obj.contactMap['20330908']['12385906'][key]
    # type(obj.dataMap['20330908']['12385906'])
    # obj.contactMap['20330908']['12385906']['label_Analysis']['edges']
    # obj.dataMap['20330908']['12385906'].keys()
    pass
