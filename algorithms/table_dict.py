from settings import Attributes
from api import base_tools
from settings import Macro

# table_dict结构=
# {
#     表名:
#         {
#             num_rows:
#             last_analyze:
#             cols:[{column_name,data_type,num_nulls}]
#         }
# }


def read_raw_table_dict(module_info):
    table_dict = {}
    target_engine = base_tools.create_engine_from_module_info(module_info)
    #读表名，表信息
    print('reading user_tables')
    attrs = Attributes.oracle_user_tables_attrs
    attrs_sql = base_tools.assemble_attrs(attrs,',')
    sql = 'select ' + attrs_sql + ' from user_tables'
    ret = base_tools.execute_sql(target_engine, sql)
    for item in ret:
        table_dict[item[0]] = base_tools.convert_tuple_dict(item,attrs,range(1,2))
        table_dict[item[0]]['cols'] = []
    print(("%d tables are red" % (len(ret),)))
    print('reading user_tab_column')
    #读列名，列信息
    attrs = Attributes.oracle_user_tab_column_attrs
    attrs_sql = base_tools.assemble_attrs(attrs,',')
    sql = 'select ' +attrs_sql + ' from user_tab_columns'
    ret = base_tools.execute_sql(target_engine, sql)
    for item in ret:
        col_dict = base_tools.convert_tuple_dict(item, attrs, range(1, len(attrs)))
        table_dict[item[0]]['cols'].append(col_dict)
        table_dict[item[0]]['num_cols'] = len(table_dict[item[0]]['cols'])
    print(("%d columns are red" % (len(ret),)))
    return table_dict


#如果列属性中的 low_value和high_value是bytes形式，且数据类型为VARCHAR2 或 CHAR，则将其解码为unicode
#如果数据发生错误，比如没有end of value，则将其值写为value
#如果high_value和low_value不是bytes，或是bytes类型但不是varchar或char，则不处理
def decode_bytes(table_dict):
    for table_name,table_info in table_dict.items():
        cols = table_info['cols']
        for col in cols:
            column_name = col['column_name']
            keys = ['low_value','high_value']
            #print(table_name,column_name,col['data_type'],col['high_value'],col['low_value'])
            for k in keys:
                if isinstance(col[k],bytes):
                    if col['data_type'] == 'VARCHAR2' or col['data_type'] == 'CHAR':
                        try:
                            col[k] = col[k].decode('utf8')
                        except Exception as e:
                            pass
                    else:
                        continue

def judge_primary_key(table_dict, table_name, col):
    if col['column_id'] != 1:
        return False

    if col['num_nulls'] != 0:
        return False

    if table_dict[table_name]['num_rows'] > 2 * col['num_distinct']:
        return False

    return True

def judge_value_uuid(value):
    if value is None:
        return False

    if len(value) < 32 or len(value) > 42:
        return False

    #如果第一个字符不是 a-z 也不是 A-Z 也不是 0-9，就返回False
    if ord(value[0]) not in range(65,91) and ord(value[0]) not in range(97,123) and ord(value[0]) not in range(48,58):
        return False

    str_list = value.split('-')
    if len(str_list) > 5:
        return False

    for s in str_list:
        if s.isalnum() is not True:
            return False
    return True

def judge_col_uuid(col):
    columns = ['high_value', 'low_value']
    res_list = []
    try:
        for column in columns:
            res_list.append(judge_value_uuid(col[column]))
        if True in res_list:
            return True
        else:
            return False

    except Exception as e:
        print(col)

def judge_col_component_type(col):
    try:
        if col['data_type'] not in ['VARCHAR2', 'CHAR']:
            return Macro.COMPONENT_NA

        high_value_type = base_tools.judge_component(col['high_value'])
        low_value_type = base_tools.judge_component(col['low_value'])
        if high_value_type == low_value_type:
            return high_value_type
        else:
            return Macro.COMPONENT_OTHERWISE
    except Exception as e:
        print(col)


def prepare_table_dict(module_info):
    #读原始table_dict
    table_dict = read_raw_table_dict(module_info)
    decode_bytes(table_dict)
    for table_name, table_info in table_dict.items():
        cols = table_info['cols']
        for col in cols:
            col['primary_key'] = judge_primary_key(table_dict, table_name, col)
            col['uuid'] = judge_col_uuid(col)
            col['component'] = judge_col_component_type(col)
    return table_dict