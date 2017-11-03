import re
import pandas as pd
import json
import numpy as np

def parse():
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
            temp['line_event'] = []
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
                temp['line_event'].append(temp2)
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
def que(x):
    if x['one'] >= x['two'] and x['one'] <= x['three']:
        return x['one']
    else:
        ''
df['que'] = df.apply(que, axis=1)
'''

def accountUsage(x):
    if x['session count'] < 3:
        return 'ignore'
    if x['session class exist count'] / (x['session count'] + 0.0) <= 0.1:
        return 'manage'
    if x['session class exist count'] / (x['session count'] + 0.0) >= 0.6:
        return 'look'
    return 'mix'

def classFocus(x):
    if x['count'] < 5:
        return 'ignore'
    if x['count'] / (x['countSum'] + 0.0) >= .5:
        return 'focus'
    if x['countSum'] > 50 and x['count'] / (x['countSum'] + 0.0) >= .25:
        return 'focus'
    if x['countSum'] > 200 and x['count'] / (x['countSum'] + 0.0) >= .2:
        return 'focus'
    return 'ignore'

if __name__ == '__main__':
    data = parse()
    # print len(data[0].keys())
    df = pd.DataFrame(data)
    # df.groupby(['contactseq'])['web_Class_amount'].sum().sort_values(ascending=False)
    writer = pd.ExcelWriter('output.xlsx')
    count_df = df.groupby(['customerseq','contactseq'], as_index=True)['web_Class_amount'].agg({'session count': np.size, 'session class exist count': lambda x: (x > 0).sum(), 'class amount sum': np.sum, 'mean': np.mean, 'max': np.max, 'min': np.min})
    count_df['usage'] = count_df.apply(accountUsage, axis = 1)
    count_df.to_excel(writer, sheet_name='count')
    # df.groupby(['customerseq','contactseq'])['web_Class_amount'].agg({'session count': np.size, 'valid class exist count': lambda x: (x > 0).sum(), 'class amount sum': np.sum, 'mean': np.mean, 'max': np.max, 'min': np.min}).to_excel(writer,'Sheet1')
    # writer.save()

    len(df.columns)
    len(df)
    len(df.sessionid.unique())
    len(df.cookieid.unique())
    len(df.customerseq.unique())
    len(df.contactseq.unique())
    # print df.contactseq.unique()[0]
    # df[df['contactseq']=='13636697'].web_Class_list
    # print len(data[0].keys())
    # print len(line_event_schema)
    # print data[0]

    # flatten web_Class_list
    class_df = pd.io.json.json_normalize(data, 'web_Class_list',['sessionid','cookieid','session_pid','timestamp','eaccountuserseq','eaccountuseremailaddressseq','cmusertrackingkey','contactseq','customerseq','marketingsiteseq','marketingentityseq','label','total_num_pages','total_session_time','first_page_contentcategory','first_page_category','first_page_name','first_page_url','first_page_duration','first_relevant_EDC_viewed','first_relevant_PGM_viewed','first_relevant_Class_viewed','first_relevant_Type_viewed','last_page_contentcategory','last_page_category','last_page_name','last_page_url','web_EDC_list','web_PGM_list','web_type_list','session_date_time','line_event'])
    len(data[0].keys())
    len(df.columns)
    len(data)
    len(df)

    colnames = class_df.columns.tolist()
    colnames[0] = 'web_Class'
    class_df.columns = colnames
    class_df.columns
    temp = class_df.groupby(['customerseq','contactseq', 'web_Class'], as_index=False)['cookieid'].count()
    temp.columns = temp.columns.tolist()[:3] + ['count']
    temp2 = temp.groupby(['customerseq','contactseq'], as_index=False)['count'].sum()
    temp2.columns = temp2.columns.tolist()[:2] + ['countSum']

    final = pd.merge(temp, temp2)
    # final.columns
    final['focus'] = final.apply(classFocus, axis=1)
    # final = final.groupby(['customerseq','contactseq', 'web_Class'], as_index=True)['count', 'countSum', 'focus'].apply(lambda x: x)
    # writer = pd.ExcelWriter('output.xlsx')
    final.set_index(['customerseq','contactseq', 'web_Class'], inplace=True)
    final.to_excel(writer, sheet_name='focus')

    events_df = pd.io.json.json_normalize(data, 'line_event',['sessionid','cookieid','session_pid','timestamp','eaccountuserseq','eaccountuseremailaddressseq','cmusertrackingkey','contactseq','customerseq','marketingsiteseq','marketingentityseq','label','total_num_pages','total_session_time','first_page_contentcategory','first_page_category','first_page_name','first_page_url','first_page_duration','first_relevant_EDC_viewed','first_relevant_PGM_viewed','first_relevant_Class_viewed','first_relevant_Type_viewed','last_page_contentcategory','last_page_category','last_page_name','last_page_url','web_EDC_list','web_PGM_list','web_Class_list','web_type_list','session_date_time'])

    events_df.groupby(['customerseq','contactseq', 'sessionid'], as_index=False).size()

    writer.save()
