from flask import Blueprint,render_template,request,flash,redirect,url_for,jsonify
from api import base_tools
from models import user
from operator import and_

from flask_login import login_user,current_user,login_required,logout_user
from api import g_query_users, g_get_user_list, g_new_user, g_edit_user, g_get_module_list,g_new_module,g_edit_module, g_query_module
from werkzeug.security import generate_password_hash, check_password_hash
from extensions import db
from models import user,module,jurisdiction
from settings import Macro, Attributes, assess_status, global_vars

report_bp = Blueprint('report',__name__)



@report_bp.route('/report',methods=['POST'])
@login_required
def report():
    try:
        ret = {}
        module_name, result_time, result_name = base_tools.parse_request_json(request, "module_name", "result_time", "result_name")
        # data = request.json
        # module_name = data['module_name']
        # result_time = data['result_time']
        # result_name = data['result_name']

        attrs = Attributes.result_store_attrs['integrity']
        sql_attrs = base_tools.assemble_attrs(attrs,',')
        if result_time == "":
            time_clause = "1"
            ov_clause   = "table_name = \'over_view\'"
        else:
            time_clause = "result_time = \'%s\'" % (result_time,)
            ov_clause = "table_name != \'over_view\'"
        module_clause = "module_name = \'%s\'" % (module_name,)

        sql = 'select %s from integrity_result_store where %s and %s and %s' % (sql_attrs, time_clause, ov_clause, module_clause)
        ret = base_tools.execute_sql(global_vars.write_engine,sql)
        result_list = []
        for r in ret:
            result_list.append(base_tools.convert_tuple_dict(r,attrs))
        return jsonify({'status':1,'result_list':result_list})

    except Exception as e:
        return jsonify({'status':0,'Err_info':e.args[0]})




#各项目通用，获取结果时间列表
@report_bp.route('/datetimelist',methods=['POST'])
@login_required
def datetime_list():
    try:
        data = request.json
        module_name = data['module_name']
        result_name = data['result_name']
        #获取数据表名
        result_table_name = global_vars.result_store_table_name[result_name]
        #组装module_clause
        if module_name == "":
            module_clause = "1"
        else:
            module_clause = "module_name=\'%s\'" % (module_name)
        # 组装sql，并执行
        sql = u'select distinct result_time, module_name from %s where %s order by module_name, result_time DESC' % (result_table_name,module_clause)
        ret = base_tools.execute_sql(global_vars.write_engine, sql)

        result_dict = {}
        for r in ret:
            module_name = r[1]
            if module_name not in result_dict.keys():
                result_dict[module_name] = []
            result_dict[module_name].append(r[0])
        return jsonify({'status': 1, 'result_list': result_dict})

    except Exception as e:
            return jsonify({'status': 0, 'func': 'datetime_list', 'Err_info': e.args[0]})

@report_bp.route('/integrity/detail_table',methods=['POST'])
@login_required
def integrity_detail_table():
    try:
        module_name, table_name = base_tools.parse_request_json(request, "module_name", "table_name")
        result_name = Macro.ASSESS_INTEGRITY
        #获取数据表名
        result_table_name = global_vars.result_store_table_name[result_name]
        # 组装sql中属性分句
        attrs = Attributes.result_store_attrs[result_name]
        sql_attrs = base_tools.assemble_attrs(attrs, ',')
        #表名clause
        table_name_clause = 'table_name = \'%s\'' % (table_name,)
        # 模块名从句，是否汇总从句
        module_clause = "module_name = \'%s\'" % (module_name,)
        # 组装sql，并执行
        sql = 'select %s from %s where %s and %s order by result_time DESC' % (
            sql_attrs, result_table_name, module_clause,table_name_clause )
        ret = base_tools.execute_sql(global_vars.write_engine, sql)
        #组装结果
        result_list = []
        for r in ret:
            result_list.append(base_tools.convert_tuple_dict(r, attrs))
        return jsonify({'status': 1, 'result_list': result_list})

    except Exception as e:
            return jsonify({'status': 0, 'func': 'detail_table', 'Err_info': e.args[0]})


@report_bp.route('/integrity/overview',methods=['POST'])
@login_required
def integrity_overview():
    try:
        module_name, result_time = base_tools.parse_request_json(request, "module_name", "result_time")
        #result_time should be any of {recent, history, date time}
        result_name = Macro.ASSESS_INTEGRITY
        #组装sql中属性分句
        attrs = Attributes.integrity_result_store_attrs
        sql_attrs = base_tools.assemble_attrs(attrs, ',')
        #获取数据表名
        result_table_name = global_vars.result_store_table_name[result_name]
        #组装时间条件从句，如果没有指定具体时间，则全部获取
        if result_time != 'recent' and result_time != 'history':    #既不是recent也不是history，那就是具体时间
            time_clause = "result_time = \'%s\'" % (result_time,)
        else:
            time_clause = "1"
        #模块名从句，是否汇总从句
        module_clause = "module_name = \'%s\'" % (module_name,)
        #组装ov_clause
        ov_clause = "table_name = \'%s\'" % ('over_view',)
        #组装sql，并执行
        sql = 'select %s from %s where %s and %s  and %s order by result_time DESC' % (
        sql_attrs, result_table_name, time_clause,ov_clause, module_clause)
        print("report_overview:",sql)
        ret = base_tools.execute_sql(global_vars.write_engine, sql)
        #获取结果，如果result_time不是history，则只获取第一条，如为history，则全部获取
        result_list = []
        for r in ret:
            result_list.append(base_tools.convert_tuple_dict(r, attrs))
            if result_time != 'history':
                break
        return jsonify({'status': 1, 'result_list': result_list})

    except Exception as e:
        return jsonify({'status': 0, 'func':'report_overview', 'Err_info': e.args[0]})




@report_bp.route('/redundancy/overview',methods=['POST'])
@login_required
def redundancy_overview():
    try:
        module_name, result_time = base_tools.parse_request_json(request, "module_name", "result_time")
        #result_time should be any of {recent, history, date time}
        result_name = Macro.ASSESS_REDUNDANCY
        #组装sql中属性分句
        attrs = ["count(*)", "result_time","avg(rate_a)"]
        sql_attrs = base_tools.assemble_attrs(attrs, ',')
        #获取数据表名
        result_table_name = global_vars.result_store_table_name[result_name]
        #组装时间条件从句，如果没有指定具体时间，则全部获取
        if result_time != 'recent' and result_time != 'history':    #既不是recent也不是history，那就是具体时间
            time_clause = "result_time = \'%s\'" % (result_time,)
        else:
            time_clause = "1"
        #模块名从句，是否汇总从句
        module_clause = "module_name = \'%s\'" % (module_name,)
        #组装sql，并执行
        sql = 'select %s from %s where %s and %s  group by result_time order by result_time DESC' % (
        sql_attrs, result_table_name, time_clause, module_clause)
        print("report_overview:",sql)
        ret = base_tools.execute_sql(global_vars.write_engine, sql)
        #获取结果，如果result_time不是history，则只获取第一条，如为history，则全部获取
        result_list = []
        for r in ret:
            result_list.append(base_tools.convert_tuple_dict(r, attrs))
            if result_time != 'history':
                break
        return jsonify({'status': 1, 'result_list': result_list})

    except Exception as e:
        return jsonify({'status': 0, 'func':'report_overview', 'Err_info': e.args[0]})

@report_bp.route('/redundancy/detail',methods=['POST'])
@login_required
def redundancy_detail():
    try:
        module_name, result_time = base_tools.parse_request_json(request, "module_name", "result_time")
        result_name = Macro.ASSESS_REDUNDANCY
        #获取数据表名
        result_store_table_name = global_vars.result_store_table_name[result_name]
        # 组装sql中属性分句
        attrs = Attributes.result_store_attrs[result_name]
        sql_attrs = base_tools.assemble_attrs(attrs, ',')
        # 模块名从句
        module_clause = "module_name = \'%s\'" % (module_name,)
        #时间从句
        result_time_clause = 'result_time = \'%s\'' % (result_time)
        # 组装sql，并执行
        sql = 'select %s from %s where %s and %s order by result_time DESC' % (
            sql_attrs, result_store_table_name, module_clause, result_time_clause)
        ret = base_tools.execute_sql(global_vars.write_engine, sql)
        #组装结果
        result_list = []
        for r in ret:
            result_list.append(base_tools.convert_tuple_dict(r, attrs))
        return jsonify({'status': 1, 'result_list': result_list})

    except Exception as e:
            return jsonify({'status': 0, 'func': 'detail_table', 'Err_info': e.args[0]})

@report_bp.route('/consistency/detail',methods=['POST'])
@login_required
def consistency_detail():
    try:
        module_name, result_time = base_tools.parse_request_json(request, "module_name", "result_time")
        result_name = Macro.ASSESS_CONSISTENCY
        #获取数据表名
        result_store_table_name = global_vars.result_store_table_name[result_name]
        # 组装sql中属性分句
        attrs = Attributes.result_store_attrs[result_name]
        sql_attrs = base_tools.assemble_attrs(attrs, ',')
        # 模块名从句
        module_clause = "module_name = \'%s\'" % (module_name,)
        #时间从句
        result_time_clause = 'result_time = \'%s\'' % (result_time)
        # 组装sql，并执行
        sql = 'select %s from %s where %s and %s order by result_time DESC' % (
            sql_attrs, result_store_table_name, module_clause, result_time_clause)
        ret = base_tools.execute_sql(global_vars.write_engine, sql)
        #组装结果
        result_list = []
        for r in ret:
            result_list.append(base_tools.convert_tuple_dict(r, attrs))
        return jsonify({'status': 1, 'result_list': result_list})

    except Exception as e:
            return jsonify({'status': 0, 'func': 'detail_table', 'Err_info': e.args[0]})

@report_bp.route('/consistency/overview',methods=['POST'])
@login_required
def consistency_overview():
    try:
        module_name, result_time = base_tools.parse_request_json(request, "module_name", "result_time")
        #result_time should be any of {recent, history, date time}
        result_name = Macro.ASSESS_CONSISTENCY
        #组装sql中属性分句
        attrs = ["count(*)", "result_time"]
        sql_attrs = base_tools.assemble_attrs(attrs, ',')
        #获取数据表名
        result_table_name = global_vars.result_store_table_name[result_name]
        #组装时间条件从句，如果没有指定具体时间，则全部获取
        if result_time != 'recent' and result_time != 'history':    #既不是recent也不是history，那就是具体时间
            time_clause = "result_time = \'%s\'" % (result_time,)
        else:
            time_clause = "1"
        #模块名从句，是否汇总从句
        module_clause = "module_name = \'%s\'" % (module_name,)
        #组装sql，并执行
        sql = 'select %s from %s where %s and %s  group by result_time order by result_time DESC' % (
        sql_attrs, result_table_name, time_clause, module_clause)
        print("report_overview:",sql)
        ret = base_tools.execute_sql(global_vars.write_engine, sql)
        #获取结果，如果result_time不是history，则只获取第一条，如为history，则全部获取
        result_list = []
        for r in ret:
            result_list.append(base_tools.convert_tuple_dict(r, attrs))
            if result_time != 'history':
                break
        return jsonify({'status': 1, 'result_list': result_list})

    except Exception as e:
        return jsonify({'status': 0, 'func':'report_overview', 'Err_info': e.args[0]})