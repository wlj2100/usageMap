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
    with open('./new_data/liangji_group_by_customer', 'r') as f:
        for line in f:
            # print line
            items = line.split('\t')
            customerseq = items[0]
            sessions_bag = items[1]

            break

print sessions_bag
sessions_bag = sessions_bag[2:-2]
sessions = sessions_bag.split('),(')
for session in sessions:
    columns = session.split(',')
    for i in range(len(columns)):
        if i < len(colName):
            print colName[i]
        else:
            print 'out of colName range'
        print columns[i]
        # if colName[i] is 'line_event':
        #     print 'enter', i, colName[i]
        #     for j in range(i, len(columns)):
        #         print j
        #         print columns[j]
        #     print 'end'

    break

72 - 4
print line
