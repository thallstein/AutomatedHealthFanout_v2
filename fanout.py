from airtable import airtable
import json

debug = 0

def debug_stuff(_debug, _info):
    if (_debug == 1):
        print('debug info: ', _info)

def get_api_and_base_info(_file):
    with open(_file) as json_file:
        data = json.load(json_file)
        API_KEY = data.get('API_KEY')
        BASE_ID = data.get('BASE_ID')
        return {"API_KEY" : API_KEY, "BASE_ID" : BASE_ID}

def clear_table(_table):
    credentials = get_api_and_base_info('/Users/tz/Google Drive File Stream/My Drive/Github/_config/automated_health_creds.json')
    API_KEY = str(credentials.get('API_KEY'))
    BASE_ID = str(credentials.get('BASE_ID'))
    debug_stuff(debug, ['BASE_ID: ', BASE_ID])
    debug_stuff(debug, ['API_KEY: ', API_KEY])

    at = airtable.Airtable(BASE_ID, _table, API_KEY)
    records = at.get_all()
    for counter, record in enumerate(records):
        at.delete(record.get('id'))


# Records in the Airtable 'Content' table can have multiple conditions addressed by a company's technology
# This function replicates the company information for each condition, publishes that to a separate table, so that
# The conditions can be searched more easily. Also loops through and gets the performance metrics for each condition

def update_public_tables_v2(_source_table ,_source_view, _target_table):
    credentials = get_api_and_base_info('/Users/tz/Google Drive File Stream/My Drive/Github/_config/automated_health_creds.json')
    API_KEY = str(credentials.get('API_KEY'))
    BASE_ID = str(credentials.get('BASE_ID'))
    debug_stuff(debug,['BASE_ID: ', BASE_ID])
    debug_stuff(debug,['API_KEY: ', API_KEY])

    at_source = airtable.Airtable(BASE_ID, _source_table, API_KEY)
    #records = at_content.get_all(view=_source_view,fields=['Summary','Organization/s (link)', 'Posting Name', 'Issue Addressed (link)'])
    content_records = at_source.get_all(view=_source_view)
    debug_stuff(debug,['content records: ', content_records])
    performance_table = airtable.Airtable(BASE_ID, 'Performance', API_KEY)
    performance_records = performance_table.get_all()
    debug_stuff(debug, ['Performance Records: ',performance_records])
    #perf_table_dict = performance_records.get('fields')
    #debug_stuff(debug, ['perf_table_dict', perf_table_dict])

    at_target_table = airtable.Airtable('appadAbcwI0Q81JUX', _target_table, 'keygBqDy9TOvoIPr2')

    for counter, record in enumerate(content_records):
        debug_stuff(debug, ['content records loop: ',counter, record])
        fields_dict = record.get('fields')
        debug_stuff(debug,['fields_dict: ',fields_dict])
        issues_addressed_list = fields_dict.get('Issue Addressed (link)')
        debug_stuff(debug, ['Issue Addressed List: ', issues_addressed_list])
        performance_metrics = fields_dict.get('Performance_Table')
        debug_stuff(debug, ['Performance metrics: ',performance_metrics])
        if issues_addressed_list:
            for issue_counter, issue_record in enumerate(issues_addressed_list):
                debug_stuff(debug,['issue addressed record: ', issue_counter, issue_record])
                fanout_record = {'Summary': fields_dict.get('Summary', ''),
                                 'Solution Name': fields_dict.get('Posting Name', ''),
                                 'Organization/s': fields_dict.get('Organization/s (link)', ''),
                                 'Issue Addressed': [issue_record],
                                 'Inputs': fields_dict.get('System or Model Inputs'),
                                 'Approvals': fields_dict.get('Approvals'),
                                 'Product Status': fields_dict.get('Content Type'),
                                 'Diagnostic Use': fields_dict.get('Diagnostic Use'),
                                 'Website':fields_dict.get('URL')}
                #fanout_record = {'Solution Name':fields_dict.get('Posting Name',''),
                #                 'Organization/s':fields_dict.get('Organization/s (link)',''),
                #                 'Issue Addressed':[issue_record]}
                #debug_stuff(debug,['    ||condition ', inner_counter+1, '||data: ',fanout_record, '||'])
                for performance_counter, performance_record in enumerate(performance_records):
                    debug_stuff(debug, ['performance record: ', performance_counter, performance_record])
                    perf_fields_dict = performance_record.get('fields')
                    perf_content_id = str(perf_fields_dict.get('Content')[0])
                    content_record_id = str(record.get('id'))
                    debug_stuff(debug,['Comparison: ',perf_content_id, content_record_id])
                    perf_condition_id = str(perf_fields_dict.get('Condition')[0])
                    debug_stuff(debug,['perf condition id: ', perf_condition_id])
                    if(perf_content_id == content_record_id and perf_condition_id == issue_record):
                        debug_stuff(debug,['Content id Match: ',record.get('id')])
                        debug_stuff(debug,['Issue id Match: ',perf_condition_id])
                        relative_performance = perf_fields_dict.get('Relative Performance', 'n/a')
                        accuracy = perf_fields_dict.get('Accuracy','')
                        precision = perf_fields_dict.get('Precision','')
                        sensitivity = perf_fields_dict.get('Recall (Sensitivity)','')
                        specificity = perf_fields_dict.get('Specificity','')
                        f1 = perf_fields_dict.get('f1','')
                        auroc = perf_fields_dict.get('AUROC','')
                        perf_metrics = {'Relative Performance':relative_performance,
                                        'Accuracy':accuracy,
                                        'Precision':precision,
                                        'Sensitivity':sensitivity,
                                        'Specificity':specificity,
                                        'f1':f1,
                                        'AUROC':auroc}
                        debug_stuff(debug,['Performance Metrics: ',perf_metrics])
                        fanout_record['Relative Performance'] = relative_performance
                        fanout_record['Accuracy'] = accuracy
                        fanout_record['Precision'] = precision
                        fanout_record['Sensitivity'] = sensitivity
                        fanout_record['Specificity'] = specificity
                        fanout_record['f1'] = f1
                        fanout_record['AUROC'] = auroc
                at_target_table.insert(fanout_record, typecast=True)

#Update Publicly Shared Table
clear_table('shared_autodx')
update_public_tables_v2('Content','python_shared_autodx','shared_autodx')

# For Testing #
#clear_table('test_shared_autodx')
#update_public_tables_v2('Content','python_shared_autodx','test_shared_autodx')