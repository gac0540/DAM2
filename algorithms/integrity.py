from api import base_tools
from settings import Attributes, global_vars, assess_status, Macro
from sqlalchemy import create_engine
from extensions import db
import threading
import copy



class integrity():
    def __init__(self, module_info):
        self.module_info = module_info
        self.target_engine = base_tools.create_engine_from_module_info(module_info)

    def check_table_integrity(self, table_dict, table_name):
        num_rows = table_dict[table_name]['num_rows']   # 条目数
        num_cols = table_dict[table_name]['num_cols']   # 列数
        num_grids = num_rows * num_cols                 # 表中grid数量
        num_null_grids = 0                              # null的grid数量
        num_null_cols = 0                               # 空列数
        num_full_cols = 0                               # 满列数量
        num_non_full_cols = 0                          # 非全空或全满列数量
        num_non_full_rows = 0                           #不满记录数
        num_full_rows = 0                               #满记录数

        col_names = []                                  #列名容器
        for column_info in table_dict[table_name]['cols']:
            col_num_nulls = column_info['num_nulls']                #列的空值数
            if col_num_nulls is None:                              #如果是None
                num_null_cols += 1
                num_null_grids += num_rows
                continue
            else:
                num_null_grids += col_num_nulls             #加空格数

            if col_num_nulls == 0:                      #无空，全满
                num_full_cols +=1
            elif col_num_nulls == num_rows:             #全空
                num_null_cols +=1
            elif col_num_nulls >0:                                       #半空
                num_non_full_cols += 1

            col_names.append(column_info['column_name'])

        assert (num_full_cols + num_non_full_cols + num_null_cols == table_dict[table_name]['num_cols'])
        attr_sql = base_tools.assemble_attrs(col_names,' is null or ')
        attr_sql += " is null "
        sql = "select count(1) from " + table_name +" where " + attr_sql
        num_non_full_rows = base_tools.execute_sql(self.target_engine,sql)[0][0]
        num_full_rows = num_rows - num_non_full_rows
        table_integrity_result = {
            "num_rows":num_rows,
            "num_cols":num_cols,
            'num_grids':num_grids,
            'num_null_grids':num_null_grids,
            'num_null_cols':num_null_cols,
            'num_full_cols':num_full_cols,
            'num_non_full_cols':num_non_full_cols,
            'num_non_full_rows':num_non_full_rows,
            'num_full_rows':num_full_rows,
            'rate_null_cols':float(num_null_cols)/float(num_cols) if num_cols !=0 else 0,
            'rate_full_rows':float(num_full_rows)/float(num_rows) if num_rows !=0 else 0,
            'rate_null_grids':float(num_null_grids)/float(num_grids) if num_grids !=0 else 0,
            'rate_null_grids2':float(num_null_grids - num_rows*num_null_cols)/float(num_grids - num_rows*num_null_cols) if (num_grids - num_rows*num_null_cols)!=0 else 0
        }
        return table_integrity_result

    def check_integrity(self):
        sql = 'delete from integrity_result where module_name = \'' + self.module_info.module_name + '\''
        db.session.execute(sql)
        table_dict = self.prepare_table_dict()
        num_tables = len(table_dict)
        num_ckd_tables = 0
        for table_name in table_dict.keys():
            print('processing table %s, num_rows is %d, num_cols is %d' % (table_name, \
                                                                           table_dict[table_name]['num_rows'],\
                                                                           table_dict[table_name]['num_cols']))
            print('%d of %d' % (num_ckd_tables, num_tables))
            table_integrity_result = self.check_table_integrity(table_dict,table_name)
            num_ckd_tables +=1
            table_integrity_result['module_name'] = self.module_info.module_name
            table_integrity_result['module_id'] = self.module_info.id
            table_integrity_result['table_name'] = table_name
            base_tools.write_dict_mysql(table_integrity_result,'integrity_result')

    @staticmethod
    def integrity_assess_summary(module_info, result_list):
        result = {
            'module_id': module_info.id,
            'module_name': module_info.module_name,
            'table_name': 'over_view',
            "num_rows": 0,
            "num_cols": 0,
            'num_grids': 0,
            'num_null_grids': 0,
            'num_null_cols': 0,
            'num_full_cols': 0,
            'num_non_full_cols': 0,
            'num_non_full_rows': 0,
            'num_full_rows': 0
        }
        num_grids_null_cols = 0  # 空列中的格数
        for r in result_list:
            result['num_rows'] += r['num_rows']
            result['num_cols'] += r['num_cols']
            result['num_grids'] += r['num_grids']
            result['num_null_grids'] += r['num_null_grids']
            result['num_null_cols'] += r['num_null_cols']
            result['num_full_cols'] += r['num_full_cols']
            result['num_non_full_cols'] += r['num_non_full_cols']
            result['num_non_full_rows'] += r['num_non_full_rows']
            result['num_full_rows'] += r['num_full_rows']
            num_grids_null_cols += r['num_rows'] * r['num_null_cols']
        result['rate_null_cols'] = float(result['num_null_cols']) / float(result['num_cols']) if result[
                                                                                                     'num_cols'] != 0 else 0
        result['rate_full_rows'] = float(result['num_full_rows']) / float(result['num_rows']) if result[
                                                                                                     'num_rows'] != 0 else 0
        result['rate_null_grids'] = float(result['num_null_grids']) / float(result['num_grids']) if result[
                                                                                                        'num_grids'] != 0 else 0
        result['rate_null_grids2'] = float(result['num_null_grids'] - num_grids_null_cols) / float(
            result['num_grids'] - num_grids_null_cols) if (result['num_grids'] - num_grids_null_cols) != 0 else 0
        base_tools.write_dict_mysql(result, 'integrity_result')


    @staticmethod
    def get_task_list(table_dict):
        task_list = []
        for key in table_dict.keys():
            task_list.append(key)
        print("%d tasks are filled" % (len(task_list),))
        return task_list



    @staticmethod
    def integrity_assess_func(module_info, table_dict, resume):
        # 开始数据完整性检查
        print("start integrity assess")

        task_code = Macro.ASSESS_INTEGRITY
        # 如果不是恢复评估，则删除之前的对应模块的记录
        # if not resume:
        sql = 'delete from ' + global_vars.result_table_name[task_code] + ' where module_name = \'' + module_info.module_name + '\''
        base_tools.execute_sql(global_vars.write_engine, sql)

        # 获取任务清单（表名放在task_list中）
        task_list = integrity.get_task_list(table_dict)
        assess_status.module_status_dict[module_info.module_name].start_stage(task_code,
                                                                              len(task_list))
        thread_list = []
        result_list = []
        for i in range(global_vars.NUM_WORKERS):
            thread_list.append(threading.Thread(target=integrity.integrity_assess_thread,
                                                args=(module_info, table_dict, task_list, result_list, i)))
            thread_list[i].start()
        for th in thread_list:
            th.join()

        #整列结果，产生over_view
        integrity.integrity_assess_summary(module_info, result_list)

        #设置完成状态
        assess_status.module_status_dict[module_info.module_name].end_stage(task_code)



    @staticmethod
    def integrity_assess_thread(module_info, table_dict, task_list, result_list, i):
        print("-------thread %d start" % (i,))
        ck_int = integrity(module_info)

        exception_counter = 0

        while len(task_list) > 0 and global_vars.stop_sign[module_info.module_name] is not True:
            try:
                table_name = task_list.pop(0)
            except Exception as e:
                print("----------thread %d is over-------" % (i,))
                break
            try:
                print("table %s is processed by thread %d" % (table_name, i))
                table_integrity_result = ck_int.check_table_integrity(table_dict, table_name)
                # 补全待写入数据库信息
                table_integrity_result['module_name'] = module_info.module_name
                table_integrity_result['module_id'] = module_info.id
                table_integrity_result['table_name'] = table_name
                base_tools.write_dict_mysql(table_integrity_result, 'integrity_result')
            except Exception as e:
                #如果发生意外，当前任务没有被处理完成并写入数据库，把该任务重新加入队列
                task_list.append(table_name)

                exception_counter += 1
                if exception_counter <= global_vars.retry_limit:
                    continue
                else:
                    return False
            assess_status.module_status_dict[module_info.module_name].done_plus(Macro.ASSESS_INTEGRITY)
            result_list.append(table_integrity_result)
        print("---------------thread %d is over-------------" % (i,))
