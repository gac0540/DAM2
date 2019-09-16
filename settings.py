

from sqlalchemy import create_engine
import multiprocessing as mp
from copy import deepcopy

class Macro():
    LOGIN_STATUS_SUCCESS        = 1
    LOGIN_STATUS_FAIL           = 0

    LOGOUT_STATUS_SUCESS        = 1

    NEW_USER_FAIL               = 0
    NEW_USER_SUCCESS            = 1
    NEW_USER_FAIL_ID_EXISTED    = 1
    NEW_USER_FAIL_DATABASE      = 2

    DEL_USER_FAIL               = 0
    DEL_USER_SUCCESS            = 1

    EDIT_USER_FAIL              = 0
    EDIT_USER_SUCCESS           = 1

    STATUS_FAIL                 = 0
    STATUS_SUCCESS              = 1

    JOB_NOT_START               = 0
    JOB_PROCESSING              = 1
    JOB_DONE                    = 2
    JOB_ABORTED                 = 3

    ASSESS_NOT_START            = 0
    ASSESS_PROCESSING           = 1
    ASSESS_DONE                 = 2
    ASSESS_ABNORMAL             = 3

    ASSESS_TABLE_DICT           = 0
    ASSESS_INTEGRITY            = 1
    ASSESS_REDUNDANCY           = 2
    ASSESS_CONSISTENCY          = 3

    COMPONENT_NA                = -1
    COMPONENT_NUMBER            = 0
    COMPONENT_ALPHA             = 1
    COMPONENT_CHINESE           = 2
    COMPONENT_LETTER            = 3
    COMPONENT_OTHERWISE         = 4


class Attributes():
    column_attrs_in_table_view = ['column_name', 'data_type', 'data_length']
    table_attrs_in_module_view = ['table_name', 'num_rows', 'avg_row_len', 'last_analyzed']

    module_attrs = ['id', 'module_name', 'database_type', 'user_name', 'password', 'ip_address', 'port', 'db_name', 'sid' ]

    integrity_result_attrs = ['id','module_id','module_name','table_name','num_rows','num_cols','num_grids',\
                               'num_null_grids','num_null_cols','num_full_cols','num_non_full_cols', 'num_full_rows','num_non_full_rows',\
                               'rate_null_cols','rate_full_rows','rate_null_grids','rate_null_grids2']

    integrity_result_store_attrs = ['id', 'result_time','module_id', 'module_name', 'table_name', 'num_rows', 'num_cols', 'num_grids', \
                              'num_null_grids', 'num_null_cols', 'num_full_cols', 'num_non_full_cols', 'num_full_rows', 'num_non_full_rows', \
                              'rate_null_cols', 'rate_full_rows', 'rate_null_grids', 'rate_null_grids2']

    redundancy_result_attrs = ['id','module_id','module_name','table_a','table_b','num_cols_a','num_cols_b','num_same_cols',\
                               'len_col_names_a','len_col_names_b','lv_distance','rate_a','rate_b']

    redundancy_result_store_attrs = ['id', 'module_id', 'module_name', 'table_a', 'table_b', 'num_cols_a', 'num_cols_b',
                               'num_same_cols', \
                               'len_col_names_a', 'len_col_names_b', 'lv_distance', 'rate_a', 'rate_b']

    consistency_result_attrs = ['id','module_id','module_name','table_A','col_A','table_B','col_B','found_rate','data_type','avg_col_len_A','avg_col_len_B','uuid']

    consistency_result_store_attrs = ['id','module_id','module_name','table_A','col_A','table_B','col_B','found_rate','data_type','avg_col_len_A','avg_col_len_B','uuid']

    result_attrs = {
        Macro.ASSESS_INTEGRITY:integrity_result_attrs,
        'integrity': integrity_result_attrs,
        Macro.ASSESS_REDUNDANCY:redundancy_result_attrs,
        'redundancy': redundancy_result_attrs,
        Macro.ASSESS_CONSISTENCY: consistency_result_attrs,
        'consistency': consistency_result_attrs
    }

    result_store_attrs = {
        Macro.ASSESS_INTEGRITY: integrity_result_store_attrs,
        'integrity': integrity_result_store_attrs,
        Macro.ASSESS_REDUNDANCY: redundancy_result_store_attrs,
        'redundancy': redundancy_result_store_attrs,
        Macro.ASSESS_CONSISTENCY: consistency_result_store_attrs,
        'consistency': consistency_result_store_attrs
    }

    #从oracle.user_tables读列名时候的字段名
    oracle_user_tables_attrs = ['table_name','num_rows','last_analyzed']
    #从roacle.user_tab_column读列信息
    #确保
    oracle_user_tab_column_attrs = ['table_name','column_name','data_type','num_nulls','column_id','high_value','low_value','avg_col_len','num_distinct']



class global_vars():
    NUM_WORKERS                 = 4
    linkword_mysql = "mysql://root:0000@localhost/dam2"
    write_engine = create_engine("mysql+mysqldb://root:0000@localhost/dam2?charset=utf8")
    stop_sign = {}
    sv_file_path = "progress.sv"

    #数据库错误重试次数
    retry_limit  = 5

    write_json_to_file                    = True
    read_json_from_file                   = False

    redundancy_thr = {
        'string_rate':  0.2,
        'cols_rate':    0.7
    }

    consistency_conf = {
        'sample_num':10,
        'found_rate':0.9
    }
    result_table_name = {
        Macro.ASSESS_INTEGRITY:"integrity_result",
        'integrity': "integrity_result",
        Macro.ASSESS_REDUNDANCY: "redundancy_result",
        'redundancy':"redundancy_result",
        Macro.ASSESS_CONSISTENCY: "consistency_result",
        'consistency':"consistency_result"
    }

    result_store_table_name = {
        Macro.ASSESS_INTEGRITY: "integrity_result_store",
        'integrity':"integrity_result_store",
        Macro.ASSESS_REDUNDANCY: "redundancy_result_store",
        'redundancy': "redundancy_result_store",
        Macro.ASSESS_CONSISTENCY: "consistency_result_store",
        'consistency': "consistency_result_store",
    }

    @staticmethod
    def init_stop_sign(stop_sign, module_name_list):
        for module_name in module_name_list:
            stop_sign[module_name] = False


class Assess_Status():
    def __init__(self):
        self.module_status_dict = {}

    def init(self, module_name_list):
        for module_name in module_name_list:
            self.module_status_dict[module_name] = Module_Assess_Status(module_name)

    def get_assess_status(self, module_name_list):
        status_list = []
        for module_name in module_name_list:
            if module_name not in self.module_status_dict.keys():
                self.module_status_dict[module_name] = Module_Assess_Status(module_name)
            status_list.append(self.module_status_dict[module_name].get_assess_status())
        ret = {}
        ret['status'] = status_list
        return ret

    def del_module_status(self,module_name):
        del self.module_status_dict[module_name]

    def get_module_status(self,module_name):
        #判断是否在module_status_dict中，如果有，则返回对应键值; #如果没有，则创建对应的键和键值，并返回
        if module_name not in self.module_status_dict.keys():
            self.module_status_dict[module_name] = Module_Assess_Status(module_name)
        return self.module_status_dict[module_name].get_assess_status()

class Module_Assess_Status():
    def __init__(self, module_name):
        print('init assess status')
        self.module_name = module_name
        self.reset(self.module_name)
        self.lock = mp.Lock()


    def reset(self, module_name):
        self.assess_status = {
            'module_name':module_name,
            'status':Macro.ASSESS_NOT_START,         #0为停止，1为正在,2为结束
            'exception':'',
            'jobs':[
                {
                    'job_name' : Macro.ASSESS_TABLE_DICT,
                    'status'    : Macro.ASSESS_NOT_START,
                    'todo'      : -1,
                    'done'      : 0
                },
                {
                    'job_name':Macro.ASSESS_INTEGRITY,
                    'status':Macro.JOB_NOT_START,
                    'todo':-1,
                    'done':0
                },
                {
                    'job_name'  :Macro.ASSESS_REDUNDANCY,
                    'status'    :Macro.ASSESS_NOT_START,
                    'todo'      :-1,
                    'done'      :0
                },
                {
                    'job_name': Macro.ASSESS_CONSISTENCY,
                    'status': Macro.ASSESS_NOT_START,
                    'todo': -1,
                    'done': 0
                }

            ]
        }

    def set_start(self,from_file = False):
        pass

    def start_stage(self, stage, todo):
        self.lock.acquire()
        if stage == Macro.ASSESS_TABLE_DICT:
            self.assess_status['status'] = Macro.ASSESS_PROCESSING
            self.assess_status['exception'] = ''
            self.assess_status['jobs'][Macro.ASSESS_TABLE_DICT]['status'] = Macro.JOB_PROCESSING
            self.assess_status['jobs'][Macro.ASSESS_TABLE_DICT]['todo'] = todo
        else:
            self.assess_status['jobs'][stage]['status'] = Macro.JOB_PROCESSING
            self.assess_status['jobs'][stage]['todo'] = todo
        self.lock.release()

    def end_stage(self,stage):
        self.assess_status['jobs'][stage]['status'] = Macro.JOB_DONE

    def get_assess_status(self):
        self.lock.acquire()
        ret = deepcopy(self.assess_status)
        self.lock.release()
        return ret

    def done_plus(self,stage):
        self.lock.acquire()
        self.assess_status['jobs'][stage]['done'] += 1
        self.lock.release()

    #0正常，-1不正常
    def end(self, quit_code, param = None ):
        self.lock.acquire()
        if quit_code == 0:  #正常退出
            self.assess_status['jobs'][-1]['status'] = Macro.JOB_DONE
            self.assess_status['status'] = Macro.ASSESS_DONE
            self.assess_status['exception'] = ''
        elif quit_code == -1:
            self.assess_status['status'] = Macro.ASSESS_ABNORMAL
            self.assess_status['exception'] = param
        self.lock.release()


    def get_job_list(self):
        return self.assess_status['jobs']

    def resume(self):
        self.assess_status['status'] = Macro.ASSESS_PROCESSING
        self.assess_status['exception'] = ''
        #设获取数据字典的已完成工作量为0
        self.assess_status['jobs'][Macro.ASSESS_TABLE_DICT]['done'] = 0

    def is_stage_done(self,stage):
        self.lock.acquire()
        if self.assess_status['jobs'][stage]['status'] == Macro.JOB_DONE:
            bret =  True
        else:
            bret = False
        self.lock.release()
        return bret

    def set_start(self):
        self.lock.acquire()
        self.assess_status['status'] = Macro.ASSESS_PROCESSING
        self.lock.release()


assess_status = Assess_Status()