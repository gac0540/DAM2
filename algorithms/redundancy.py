from api import base_tools
import Levenshtein as lv
from settings import assess_status, Macro, global_vars
from api import base_tools

class redundancy():
    def __init__(self, module_info, target_engine):
        self.module_info = module_info

    @staticmethod
    def prepare_check_data(module_info, table_dict):
        tables_data = []
        for table_name, table_property in table_dict.items():
            temp_dict = {}
            temp_dict['table_name'] = table_name
            temp_dict['cols'] = []
            for column in table_property['cols']:
                temp_dict['cols'].append(column['column_name'])
            temp_dict['num_cols'] = len(temp_dict['cols'])
            temp_dict['col_string'] = "".join(temp_dict['cols'])
            temp_dict['len_col_string'] = len(temp_dict['col_string'])
            tables_data.append(temp_dict)
        return tables_data

    @staticmethod
    def check_redundancy(module_info, table_dict, task_list):
        tables_data = task_list
        result_list = []
        for i in range(len(tables_data)):
            for j in range(i+1,len(tables_data)):
                dis = lv.distance(tables_data[i]['col_string'], tables_data[j]['col_string'])
                #intersection_cols is a set
                intersection_cols = set(tables_data[i]['cols']).intersection(set(tables_data[j]['cols']))
                num_same_cols = len(intersection_cols)
                result = {
                    'module_id':module_info.id,
                    'module_name': module_info.module_name,
                    'table_a':tables_data[i]['table_name'],
                    'table_b':tables_data[j]['table_name'],
                    'num_cols_a':tables_data[i]['num_cols'],
                    'num_cols_b':tables_data[j]['num_cols'],
                    'num_same_cols':num_same_cols,
                    'len_col_names_a':tables_data[i]['len_col_string'],
                    'len_col_names_b':tables_data[j]['len_col_string'],
                    'lv_distance':dis,
                    'rate_a':float(dis)/float(tables_data[i]['len_col_string']),
                    'rate_b':float(dis)/float(tables_data[j]['len_col_string'])
                }

                #判断是否满足门槛条件
                if result['rate_a'] > global_vars.redundancy_thr['string_rate'] and result['rate_a'] > global_vars.redundancy_thr['string_rate']:
                    continue
                if result['num_same_cols']/float(result['num_cols_a']) < global_vars.redundancy_thr['cols_rate'] and result['num_same_cols']/float(result['num_cols_b']) < global_vars.redundancy_thr['cols_rate']:
                    continue
                result_list.append(result)
            assess_status.module_status_dict[module_info.module_name].done_plus(Macro.ASSESS_REDUNDANCY)
        return result_list

    @staticmethod
    def redundancy_assess_func(module_info, table_dict,  resume):
        task_code = Macro.ASSESS_REDUNDANCY
        sql = 'delete from ' + global_vars.result_table_name[task_code] + ' where module_name = \'' + module_info.module_name + '\''
        base_tools.execute_sql(global_vars.write_engine, sql)
        task_list = redundancy.prepare_check_data(module_info, table_dict)
        assess_status.module_status_dict[module_info.module_name].start_stage(task_code,
                                                                              len(task_list))


        result_list = redundancy.check_redundancy(module_info,table_dict, task_list)
        result_list.sort(key=lambda elem: elem['rate_a'])
        print('%d items are found in redundancy asssess' % (len(result_list),))
        base_tools.write_many_dict_mysql(result_list,global_vars.result_table_name[task_code])

        assess_status.module_status_dict[module_info.module_name].end_stage(task_code)





