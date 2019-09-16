from api import base_tools
import Levenshtein as lv
from settings import assess_status, Macro, global_vars
from api import base_tools
import threading
import time

class consistency():
    def __init__(self, module_info):
        self.module_info = module_info
        self.target_engine = base_tools.create_engine_from_module_info(module_info)

    #
    @staticmethod
    def prejudge_cols(module_info, target_engine, table_dict, primary_table, primary_col, table_name_B, col):
        #如果平均列长度差比较多
        if primary_col['avg_col_len'] > col['avg_col_len']:
            big_avg_col_len = primary_col['avg_col_len']
            small_avg_col_len = col['avg_col_len']
        else:
            small_avg_col_len = primary_col['avg_col_len']
            big_avg_col_len = col['avg_col_len']
        if small_avg_col_len == 0:
            return False
        if big_avg_col_len / small_avg_col_len > 3:
            return False
        if big_avg_col_len / small_avg_col_len > 2 and small_avg_col_len>5:
            return False
        if big_avg_col_len / small_avg_col_len > 1.5 and small_avg_col_len>10:
            return False
        if big_avg_col_len / small_avg_col_len > 1.2 and small_avg_col_len>20:
            return False

        if table_dict[primary_table]['num_rows'] == 0:
            return False

        #如果成分不一致，返回false，如果成分一致，但是都是otherwise，也返回false
        if primary_col['component'] != col['component']:
            return False
        elif primary_col['component'] == Macro.COMPONENT_OTHERWISE:
            return False



        #如果col是B表主键
        if col['primary_key']:
            return False
        #如果数据类型不一致
        if primary_col['data_type'] != col['data_type']:
            return False
        #如果uuid属性不一致
        if primary_col['uuid'] != col['uuid']:
            return False

        return True

    # 用table_name_A的主键检测table_name_B的非主键
    @staticmethod
    def judge_tables(module_info, target_engine, table_dict, table_name_A,table_name_B,task_list, list_to_sys_db):
        primary_col = None
        for col in table_dict[table_name_A]['cols']:
            if col['primary_key']:
                primary_col = col
                break
        #如果TA没有主键
        if primary_col == None:
            return


        #轮询对比table_name_B中的列
        for col in table_dict[table_name_B]['cols']:
            if consistency.prejudge_cols(module_info, target_engine, table_dict, table_name_A, primary_col,table_name_B, col):
                task_list.append(
                    {
                        "table_name_A":table_name_A,
                        "primary_col":primary_col,
                        "table_name_B":table_name_B,
                        "col":col
                    }
                )
                list_to_sys_db.append(
                    {
                        "table_name_A": table_name_A,
                        "col_A": primary_col['column_name'],
                        "table_name_B": table_name_B,
                        "col_B": col['column_name']
                    }
                )


    @staticmethod
    def consistency_assess_func(module_info, table_dict, resume):
        task_code = Macro.ASSESS_CONSISTENCY
        sql = 'delete from ' + global_vars.result_table_name[
            task_code] + ' where module_name = \'' + module_info.module_name + '\''
        base_tools.execute_sql(global_vars.write_engine, sql)


        table_list = list(table_dict.keys())
        list_to_sys_db = []     #写数据库列表
        task_list = []          #任务列表
        task_code = Macro.ASSESS_CONSISTENCY

        target_engine =  base_tools.create_engine_from_module_info(module_info)
        for table_name_A in table_list:
            if table_dict[table_name_A]['num_rows'] == 0:
                continue

            for table_name_B in table_list:
                if table_dict[table_name_B]['num_rows'] == 0:
                    continue
                if table_name_A == table_name_B:
                    continue
                #用table_name_A的主键检测table_name_B的非主键
                consistency.judge_tables(module_info, target_engine, table_dict, table_name_A,table_name_B,task_list,list_to_sys_db)
        base_tools.write_many_dict_mysql(list_to_sys_db, 'consistency_task')
        assess_status.module_status_dict[module_info.module_name].start_stage(task_code,
                                                                              len(task_list))

        thread_list = []
        result_list = []
        for i in range(global_vars.NUM_WORKERS):
            thread_list.append(threading.Thread(target=consistency.consistency_assess_trhead,
                                                args=(module_info, target_engine, table_dict, task_list, result_list, i, len(task_list))))
            thread_list[i].start()
        for th in thread_list:
            th.join()


    #一致性分析线程
    #module_info 为需要被分析的模块信息
    #target_engine 为需要被分析的模块的数据库引擎，通过该引擎建立数据库连接
    #table_dict为数据字典，保存数据库的概览信息
    #task_list为任务列表，线程每次从task_list中取出任务，task_list为list类型，多个线程共用，是线程安全的
    #result_list用于保存结果，多个线程共用，是线程安全的
    #i表示第i个线程
    #num_total_tasks表示初始状态，共有多少个任务，用于控制台输出用
    @staticmethod
    def consistency_assess_trhead(module_info, target_engine, table_dict, task_list, result_list, i, num_total_tasks):
#        print("---------------consistency thread %d starts-------------" % (i,))
        #建立连接，每个线程开始的时候建立一次连接，以后一直使用
        conn = target_engine.connect()
        #循环条件，当task_list不为空，并且stop_sign不为True
        while len(task_list) > 0 and global_vars.stop_sign[module_info.module_name] is not True:
            start_time = time.time()
            try:
            #尝试从task_list首位置获取任务，如果失败，说明task_list空，则线程结束，break
                task = task_list.pop(0)
            except Exception as e:
                print("----------thread %d is over-------" % (i,))
                break
            try:
                # if task['table_name_A'] == 'CSTEUQCONSUMER_V' and task['primary_col']['column_name'] == 'MRID':
                #     continue
                # 成功获取到任务，计算随机采样比例，
                # num_row_b为非主键表的条目数
                # sample_num为需要采样的数量
                # sample_rate为需要采样的比例，如果超过100，则设为99.99
                num_row_b = table_dict[task['table_name_B']]['num_rows']
                sample_num = global_vars.consistency_conf['sample_num']
                sample_rate= 100 * sample_num / num_row_b + 1
                if sample_rate >= 100:
                    sample_rate = 99.99
                #第一个参数为非主键的列名，
                #第二个参数为非主键的表名，
                #第三个参数为抽样率
                #第四个参数为抽样数
                sql = 'select count(*) from {} sample ({:.2f}) where rownum <= {} '.format(
                    task['table_name_B'],
                    sample_rate,
                    sample_num
                )
                #base_number为实际取到的抽样条目数
                base_number = base_tools.execute_sql_conn(conn,sql)[0][0]

                sub_sql = 'select {} from {} sample ({:.2f}) where rownum <= {} '.format(
                    task['col']['column_name'],
                    task['table_name_B'],
                    sample_rate,
                    sample_num
                )
                #组装sql
                #第一个参数主表名
                #第二个参数，主键名
                #第三个参数，value_str
                sql = 'select count(*) from {} where {} in ({})'.format(
                    task['table_name_A'],
                    task['primary_col']['column_name'],
                    sub_sql
                )
                #print(sql)
                result = base_tools.execute_sql_conn(conn,sql)
                #result_number为样本在主键中存在条目数,found_rate为存在比例
                result_number = result[0][0]
                found_rate = result_number / base_number
                #如果found_rate大于门槛值，则保存结果
                if found_rate > global_vars.consistency_conf['found_rate']:
                    #创建结果对象
                    result = {
                        'module_id':module_info.id,
                        'module_name':module_info.module_name,
                        'table_A':task['table_name_A'],
                        'col_A':task['primary_col']['column_name'],
                        'table_B':task['table_name_B'],
                        'col_B':task['col']['column_name'],
                        'found_rate': found_rate,
                        'data_type':task['primary_col']['data_type'],
                        'avg_col_len_A': task['primary_col']['avg_col_len'],
                        'avg_col_len_B': task['col']['avg_col_len'],
                        'uuid':task['primary_col']['uuid']
                    }
                    #结果写入数据库中,结果保存至result_list
                    base_tools.write_dict_mysql(result, global_vars.result_table_name[Macro.ASSESS_CONSISTENCY])
                    result_list.append(result)

                #无论是否满足阈值，都要状态值+1
                assess_status.module_status_dict[module_info.module_name].done_plus(Macro.ASSESS_CONSISTENCY)
                end_time = time.time()
                Time = end_time - start_time
                counter = num_total_tasks - len(task_list)
                print("{}/{}\tFound:{}\tTime:{}\tby:{}".format(counter,num_total_tasks,len(result_list),Time,i))
            except Exception as e:
                conn.close()
                raise Exception(e)
        conn.close()
        print("---------------consistency thread %d is over-------------" % (i,))



