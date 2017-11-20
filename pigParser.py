import re
import pandas as pd
import json
import numpy as np
# import operator
import time
import datetime
from usageMap import UsageMap
from collections import Counter
import pprint
colName = ['sessionid','cookieid','session_pid','firsttimestamp','firstreferringurl','firstdestinationurl','firstreferraltype','timestamp','referralname','referralurl','referraltype','referralsource','city','state','country','dma','secondleveldomain','browsertype','javascriptversion','language','screenresolution','operatingsystemname','mobilenetwork','mobiledevice','devicetype','eaccountuserseq','eaccountuseremailaddressseq','cmusertrackingkey','contactseq','customerseq','marketingsiteseq','marketingentityseq','label','total_num_pages','total_num_elements','num_product','num_search','num_shop','num_account_center','num_homepage','num_solutions_and_services','num_hubs','num_custom_platinum_pages','num_brands','num_other','num_pdf','num_video','num_other_media_library_digital_assets','total_session_time','first_page_contentcategory','first_page_category','first_page_name','first_page_url','first_page_duration','first_relevant_EDC_viewed','first_relevant_PGM_viewed','first_relevant_Class_viewed','first_relevant_Type_viewed','last_page_contentcategory','last_page_category','last_page_name','last_page_url','web_EDC_list','web_PGM_list','web_Class_list','web_type_list','line_event','session_date_time']
if __name__ == '__main__':
    dataMap = {}
    with open('./new_data/liangji_group_by_customer', 'r') as f:
        count = 0
        for line in f:
            count += 1
            # print line
            items = line.split('\t')
            customerseq = items[0]
            if customerseq not in dataMap:
                dataMap[customerseq] = {}
            sessions_bag = items[1][2:-2]
            sessions_bag = re.sub(r'{.*}', '', sessions_bag)
            sessions = sessions_bag.split('),(')
            for i in range(len(sessions)):
                session = {}
                columns = sessions[i].split(',')
                for j in range(len(columns)):
                    try:
                        session[colName[j]] = columns[j]
                    except Exception as e:
                        print j - 1, colName[j - 1], columns[j - 1]
                        print j, columns[j]
                        print count, i
                        print sessions[i]
                        raise

                    # if j < len(colName):
                    #     print colName[j]
                    # else:
                    #     print 'out of colName range'
                    # print columns[j]
                eaccountuserseq = session['eaccountuserseq']
                if eaccountuserseq not in dataMap[customerseq]:
                    dataMap[customerseq][eaccountuserseq] = {}
                sessionid = session['sessionid']
                dataMap[customerseq][eaccountuserseq][sessionid] = session

            # break




###########
## debug ##
###########

line3 = ''
with open('./new_data/liangji_group_by_customer', 'r') as f:
    count = 0
    for line in f:
        count += 1
        if count == 3:
            line3 = line
            break

print line3

sessions_bag = line3.split('\t')[1][2:-2]
print sessions_bag

sessions_bag = re.sub(r'{.*}', '', sessions_bag)
print sessions_bag
