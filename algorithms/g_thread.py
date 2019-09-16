from algorithms.integrity import integrity
from algorithms.table_dict import prepare_table_dict
import threading
from settings import global_vars, assess_status, Attributes
from api import base_tools
from settings import Macro
import json, pickle
from algorithms.redundancy import redundancy
from algorithms.consistency import consistency




def assess_thread(module_info, resume):
    try:
        #初始化任务列表
        assess_tasks = {
            Macro.ASSESS_INTEGRITY: (integrity.integrity_assess_func, "Integrity"),
            Macro.ASSESS_REDUNDANCY: (redundancy.redundancy_assess_func, "Redundancy"),
            Macro.ASSESS_CONSISTENCY: (consistency.consistency_assess_func, "Consistency")
        }

        #获取字典
        #不分状态，都要重新获得table_dict

        if global_vars.read_json_from_file:
            print("read table_dict from json file")
            with open("table_dict.bin", "rb") as f:
                table_dict = pickle.load(f)
        else:
            print("read table_dict from database")
            assess_status.module_status_dict[module_info.module_name].start_stage(Macro.ASSESS_TABLE_DICT, 1)
            table_dict = prepare_table_dict(module_info)
            assess_status.module_status_dict[module_info.module_name].done_plus(Macro.ASSESS_TABLE_DICT)
            assess_status.module_status_dict[module_info.module_name].end_stage(Macro.ASSESS_TABLE_DICT)
        if global_vars.write_json_to_file:
            with open("table_dict.bin", "wb") as f:
                pickle.dump(table_dict, f)
            print("saved table_dict")

        for task, task_info in assess_tasks.items():
            task_code = task
            task_func = task_info[0]
            task_name = task_info[1]

            #如果当前任务没有做完，则继续做
            if assess_status.module_status_dict[module_info.module_name].is_stage_done(task_code):
                print("%s has been done in the previous assess, so it is skipped"%(task_name,))
                continue
            print('start assess %s' % (task_name,))

            #开始数据完整性检查
            print("start to assess_func:%s"%(task_name,))
            task_func(module_info,table_dict,resume)
            assess_status.module_status_dict[module_info.module_name].end_stage(task_code)

        transfer_result_store(assess_tasks, module_info)

    except Exception as e:
        assess_status.module_status_dict[module_info.module_name].end(-1, e.args[0])
        print("assess thread is finished with exception %s" % (e.args[0]))
        return

    #正常退出
    if global_vars.stop_sign[module_info.module_name] is True:
        print("assess thread is finished with a stop sign")
        global_vars.stop_sign[module_info.module_name] = False
        assess_status.module_status_dict[module_info.module_name].end(-1, "stop sign")
        return

    assess_status.module_status_dict[module_info.module_name].end(0)
    print("assess thread is finished normally")


import datetime
def transfer_result_store(assess_tasks, module_info):
    try:
        now_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        now_time ="\"%s\"" % (now_time,)
        for task_code in assess_tasks.keys():
            sql_result_attrs = ",".join(Attributes.result_attrs[task_code][1:])
            module_name = module_info.module_name
            result_table = global_vars.result_table_name[task_code]
            result_store_table = global_vars.result_store_table_name[task_code]
            module_name_clause = "module_name = \'%s\'" % (module_name,)
            sql = 'insert into %s (result_time, %s) select %s as result_time, %s from %s where %s' % (
                result_store_table,
                sql_result_attrs,
                now_time,
                sql_result_attrs,
                result_table,
                module_name_clause
            )
            print(sql)
            base_tools.execute_sql(global_vars.write_engine,sql)



    except Exception as e:
        print('transfer result error!')
        print(e.args[0])

def save_progress(module_name, sz_path):
    with open(sz_path,"r") as rf:
        tables_done_dict = json.load(rf)

    #确定当前进度
    job = None
    for j in assess_status.module_status_dict[module_name].get_job_list():
        if j['status'] == Macro.JOB_PROCESSING or j['status'] ==Macro.JOB_ABORTED:
            job = j
            break

    if job['job_name'] == Macro.ASSESS_TABLE_DICT:
        ret = [('',)]
    if job['job_name'] == Macro.ASSESS_INTEGRITY:
        sql = 'select table_name from integrity_result'
        ret = base_tools.execute_sql(global_vars.write_engine,sql)

    #提取已经处理过的表的表名
    tables_done = []
    for r in ret:
        tables_done.append(r[0])
    tables_done_dict[module_name] = tables_done
    #保存文件
    with open(sz_path,"w") as f:
        json.dump(tables_done_dict,f)