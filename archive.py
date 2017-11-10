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

    # sorted(obj.contactMap['20330908']['12385906']['class_focus'].items(), key=lambda x: x[1], reverse=True)



# def extend(usageMap):
#     for company in usageMap:
#         for account in usageMap[company]:
#             sessionDict = usageMap[company][account]
#             # get session count, class exist count
#             for


    # colHeads =  ['sessionid','cookieid','session_pid','timestamp','eaccountuserseq','eaccountuseremailaddressseq','cmusertrackingkey','contactseq','customerseq','marketingsiteseq','marketingentityseq','label','total_num_pages','total_session_time','first_page_contentcategory','first_page_category','first_page_name','first_page_url','first_page_duration','first_relevant_EDC_viewed','first_relevant_PGM_viewed','first_relevant_Class_viewed','first_relevant_Type_viewed','last_page_contentcategory','last_page_category','last_page_name','last_page_url','web_EDC_list','web_PGM_list','web_Class_list','web_type_list','line_event','session_date_time']
    # line_event_schema = ['flag','index','date_time','event_duration','pagename','pageid','contentcategory','contentcategoryid','contentcategorytop','pageurl','searchresultscount','attribute7','itemcode','itemseq','branddescription','itemtypecode','itemclasscode','itemgroupmajorcode','manufacturercode','elementname','elementcategory','horiz_campaign','horiz_theme','sw_mgmt_campaign']


# test = [i for i in range(100)]
# len(np.histogram(test)[0])
# len(np.histogram(test)[1])
# np.histogram(test, bins=5)
#
#
#
#
# a = [1, 2, 2, 1, 2]
# dictionary = Counter(a)
#
# print type(dictionary)
# print np.sum(dictionary.values())
# dictionary.keys()
# dictionary[3] = 1
# print dictionary
# dictionary.keys()
# dictionary.values()
