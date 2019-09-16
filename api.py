from models import user, module, jurisdiction
from settings import Macro, Attributes, global_vars
from sqlalchemy import create_engine
from extensions import db
from werkzeug.security import generate_password_hash, check_password_hash
from sqlalchemy.pool import NullPool


class base_tools():
    def __init__(self):
        pass

    @staticmethod
    def create_engine_from_module_info(module_info):
        linkword = ''
        if module_info.database_type == 'oracle':
            linkword = 'oracle+cx_oracle://'+module_info.user_name\
                + ':' + module_info.password\
                + '@' + module_info.ip_address\
                + ':' + module_info.port\
                + '/' + module_info.sid
        #print(linkword)
        engine = create_engine(linkword, poolclass=NullPool)
        return engine

    @staticmethod
    def execute_sql(engine, sql):
        #print(sql)
        conn = engine.connect()
        ret = conn.execute(sql)
        if ret.returns_rows is True:
            ret = ret.fetchall()
        conn.close()
        return ret

    @staticmethod
    def execute_sql_conn(conn,sql):
        ret = conn.execute(sql)
        if ret.returns_rows is True:
            ret = ret.fetchall()
        return ret


    @staticmethod
    def assemble_attrs(attrs, split):
        string = split.join(attrs)
        return string

    @staticmethod
    def convert_obj_dict( obj, attrs = None):
        ret = {}
        if attrs is not None:
            for attr in attrs:
                ret[attr] = vars(obj)[attr]
            return ret
        else:
            return vars(obj)

    @staticmethod
    def convert_tuple_dict( tuple, attrs,myrange=None):
        if myrange is None:
            myrange = range(len(attrs))
        ret = {}

        for i in myrange:
            ret[attrs[i]] = tuple[i]

        return ret

    @staticmethod
    def write_dict_mysql(dict, table_name):
        values = []
        for k,v in dict.items():
            if isinstance(v,str):
                values.append('\'%s\''%(v,))
            else:
                values.append(str(v))
        col_str = ",".join(dict.keys())
        value_str=",".join(values)
        sql = 'insert into %s (%s) values (%s)' % (table_name,col_str,value_str)
        #print(sql)
        conn = global_vars.write_engine.connect()
        conn.execute(sql)
        conn.close()


    @staticmethod
    def write_many_dict_mysql(dict_list, table_name):
        if len(dict_list) == 0:
            return
        #准备数据
        values = []
        for d in dict_list:
            t = tuple(d.values())
            values.append(t)
        #准备sql语句
        col_str = ",".join(dict_list[0].keys())
        num_vars = len(dict_list[0])
        #拼装sql语句
        value_ps_list = [r"%s" for i in range(num_vars)]
        value_str = ",".join(value_ps_list)
        sql = "insert into %s (%s) values (%s)" % (table_name,col_str,value_str)
        #执行executemany
        try:
            conn = global_vars.write_engine.raw_connection()
            cursor = conn.cursor()
            cursor.executemany(sql,values)
            conn.commit()
            cursor.close()
            conn.close()
        except Exception as e:
            cursor.close()
            conn.close()
            print(e.args)

    @staticmethod
    def parse_request_json(request,*keys):
        if request.is_json is not True:
            raise Exception("no json is posted")
        else:
            data = request.json

        value_list = []
        for key in keys:
            value = data.get(key)
            if value is None:
                raise Exception("key %s does not exist"%(key))
            else:
                #可以在这里对value进行合法性判断
                value_list.append(value)
        return tuple(value_list)



        if True in res_list:
            return True
        else:
            return False

    @staticmethod
    def judge_uuid(item):
        if isinstance(item,bytes):
            item = item.decode('utf-8')
        if len(item) < 32 or len(item) > 42:
            return False
        STR = 'abcdefABCDEF0123456789-'
        for c in item:
            if c not in STR:
                return False
        return True

    @staticmethod
    def judge_pure_number(val):
        STR = '0123456789'
        for c in val:
            if c not in STR:
                return False
        return True

    @staticmethod
    def judge_pure_char(val):
        STR = 'abcdefghijklmnopqrstuvwxyz'
        STR+= 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
        for c in val:
            if c not in STR:
                return False
        return True


    @staticmethod
    def judge_component(val):
        if isinstance(val,str) is not True:
            return Macro.COMPONENT_NA
        num_val = None

        #如果纯数字
        if val.isdigit():
            return Macro.COMPONENT_NUMBER

        #如果不纯字符
        if val.isalpha() is not True:
            return Macro.COMPONENT_OTHERWISE
        #初始状态为None
        type_abc = False
        type_chinese = False
        for c in val:
            if ord(c) in range(65,91) or ord(c) in range(97,123):
                type_abc = True
            else:
                type_chinese = True
        if type_abc and type_chinese:
            return Macro.COMPONENT_OTHERWISE
        elif type_abc:
            return Macro.COMPONENT_LETTER
        else:
            return Macro.COMPONENT_CHINESE


#产生、验证哈希密码，flask书272页

def g_query_users(user_id):
    return user.query.get(user_id)

def g_query_module(module_name):
    module_info = get_module_info(module_name)
    if module_info == Macro.STATUS_FAIL:
        return None
    return module_info


def g_get_module_list():
    qret = module.query.all()
    attrs = Attributes.module_attrs
    module_list = []
    for obj in qret:
        dict = base_tools.convert_obj_dict(obj, attrs)
        module_list.append(dict)
    return module_list

def g_get_module_name_list():
    module_name_list = []
    module_list = g_get_module_list()
    for m in module_list:
        module_name_list.append(m['module_name'])
    return module_name_list




def g_get_module_data(name):
    module_info = get_module_info(name)
    if module_info == Macro.STATUS_FAIL:
        return module_info
    engine = get_module_engine(module_info)
    ret = {}
    ret['module_OV'] = get_module_OV(engine,module_info)
    ret['module_tables'] = get_tables_by_module_info(engine, module_info)
    return ret

def g_get_table_data(module_name, table_name):
    module_info = get_module_info(module_name)
    if module_info == Macro.STATUS_FAIL:
        return module_info
    engine = get_module_engine(module_info)
    ret = {}
    ret['table_OV'] = ''
    ret['table_columns'] = get_columns_by_table_name(engine, module_info, table_name)

def get_columns_by_table_name(engine, module_info, table_name):
    attrs = Attributes.column_attrs_in_table_view
    sub_sql_attrs = base_tools.assemble_attrs(attrs, ',')
    sql = 'select ' + sub_sql_attrs + ' from all_tab_columns where table_name = \'' + table_name + '\''
    ret = engine.execute(sql).fetchall()
    return ret

def g_get_user_list():
    qret = user.query.all()

    user_list = []
    for obj in qret:
        user_list.append(g_get_user(obj))
    return user_list

def g_get_user(user_obj):
    attrs = ['user_id', 'user_role', "user_name"]
    dict = base_tools.convert_obj_dict(user_obj, attrs)
    dict['user_permissions'] = get_user_jurisdiction(user_obj.user_id)
    return dict

def get_user_jurisdiction(user_id):
    qret = jurisdiction.query.filter(jurisdiction.user_id==user_id).all()
    jd_dict = {}
    for q in qret:
        module_name = module.query.filter(module.id==q.module_id).first().module_name
        jd_dict[module_name]=q.jd_code
    return jd_dict

def g_new_user(data):
    qret = user(user_id=data['user_id'], user_name=data['user_name'], user_role=data['user_role'],
                 hashed_password=generate_password_hash(data['password']))
    db.session.add(qret)
    update_user_permissions(data['user_id'], data['user_permissions'])

def g_new_module(data):
    qret = module(module_name=data['module_name'], database_type=data['database_type'], user_name=data['user_name'],\
                password=data['password'],ip_address=data['ip_address'],port=data['port'],db_name=data['db_name'],sid=data['sid'])
    db.session.add(qret)

def g_edit_user(data):
    user.query.filter(user.user_id == data['user_id']).update({"user_id": data['user_id'], \
                                                               "user_name": data['user_name'], \
                                                               "user_role": data['user_role']\
                                                                # ,"hashed_password": generate_password_hash(
                                                                #    data['password'])
                                                               })
    update_user_permissions(data['user_id'], data['user_permissions'])

def g_reset_password(data):
    user.query.filter(user.user_id == data['user_id']).update({"hashed_password": generate_password_hash(
                                                                    data['password'])})

def g_edit_module(data):
    module.query.filter(module.module_name == data['module_name']).update({"database_type": data['database_type'], \
                                                                           "user_name": data['user_name'], \
                                                                           "password": data['password'], \
                                                                           "ip_address": data['ip_address'], \
                                                                           "port": data['port'], \
                                                                           "db_name": data['db_name'], \
                                                                           "sid": data['sid']})

def update_user_permissions(user_id, user_permissions):
    #删除所有该id已有权限
    qret = jurisdiction.query.filter(jurisdiction.user_id==user_id).all()
    for q in qret:
        db.session.delete(q)
    #加入新的权限
    for k, v in user_permissions.items():
        module_id = module.query.filter(module.module_name==k).first().id
        jd = jurisdiction(user_id=user_id,
                          module_id=module_id,
                          jd_code=v)
        db.session.add(jd)

#获取module概览
def get_module_OV(engine, module_info):
    return {}

def get_tables_by_module_info(engine, module_info):
    attrs = Attributes.table_attrs_in_module_view
    sub_sql_attrs = base_tools.assemble_attrs(attrs, ',')
    sql = 'select ' + sub_sql_attrs + ' from user_tables'
    ret = engine.execute(sql).fetchall()
    return ret

def get_module_info(name):
    qret = module.query.filter(module.module_name == name).first()
    if qret is None:
        return Macro.STATUS_FAIL
    attrs = Attributes.module_attrs
    return base_tools.convert_obj_dict(qret, attrs)

def get_module_engine(module_info):
    linkword = get_linkword_by_module_info(module_info)
    engine = create_engine(linkword)
    return engine

def get_linkword_by_module_info(module_info):
    linkword = ''
    if module_info['database_type'] == 'oracle':
        pass
    elif module_info['database_type'] == 'mysql':
        linkword = 'mysql+mysqldb://' \
                   + module_info['user_name']\
                   + ':' + module_info['password']\
                   + '@' + module_info['ip_address']\
                   + ':' + module_info['port']\
                   + '/' + module_info['db_name']
    else:
        pass
    return linkword

