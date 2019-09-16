from flask import Blueprint,render_template,request,flash,redirect,url_for,jsonify
from flask_login import login_user,current_user,login_required,logout_user
from api import g_get_module_name_list, base_tools
from werkzeug.security import generate_password_hash, check_password_hash
from extensions import db
from models import user,module,jurisdiction
from settings import Macro
from algorithms.integrity import integrity
from models import module
from multiprocessing import Process
from algorithms.g_thread import assess_thread,global_vars
from settings import assess_status, Attributes
import copy
import threading
import os
#coding=utf-8


quality_bp = Blueprint('quality',__name__)

@quality_bp.route('/assess',methods=['POST'])
@login_required
def assess():
    try:
        data = request.json
        #如果进程正在处理
        if assess_status.module_status_dict[data['module_name']].get_assess_status()['status'] == Macro.ASSESS_PROCESSING:
            return jsonify({'status':0,'Err_info':'need to stop current assess before start a new one'})

        # if data['resume'] == 1:
        #     print("resuming assess process")
        #     assess_status.module_status_dict[data['module_name']].resume()
        # else:
        #     assess_status.module_status_dict[data['module_name']].reset(data['module_name'])                    #重新干，需要reset status
        #     print("restarting new assess with module_name:%s" %(data['module_name']))
        assess_status.module_status_dict[data['module_name']].reset(data['module_name'])  # 重新干，需要reset status
        print("start a new assess with module_name:%s" % (data['module_name']))

        module_info = module.query.filter(module.module_name == data['module_name']).first()
        th = threading.Thread(target=assess_thread, args= (module_info,data['resume']))
        global_vars.handle_assess_thread = th
        th.start()
        assess_status.module_status_dict[data['module_name']].set_start()
    except Exception as e:
        return jsonify({'status':Macro.STATUS_FAIL,'Err_info':e.args[0]})

    print("assess thread started")
    return jsonify({"status":Macro.STATUS_SUCCESS})

@quality_bp.route('/status',methods=['GET'])
@login_required
def get_status():
    module_name_list = g_get_module_name_list()
    status = assess_status.get_assess_status(module_name_list)
    return jsonify(status)

@quality_bp.route('/module_status',methods=['POST'])
@login_required
def get_module_status():
    try:
        data = request.json
        module_name = data['module_name']
        module_name_list = g_get_module_name_list()
        if module_name not in module_name_list:
            return jsonify({'status': Macro.STATUS_FAIL, 'Err_info': 'no such module'})


        #如果assess_status中没有对应的module_name键，则会创建相应的键
        module_status = assess_status.get_module_status(module_name)
        return jsonify({'status':Macro.STATUS_SUCCESS,'module_status':module_status})


    except Exception as e:
        return jsonify({'status':Macro.STATUS_FAIL,'Err_info':e.args[0]})



@quality_bp.route('/stop',methods=['POST'])
@login_required
def kill():
    try:
        module_name, = base_tools.parse_request_json(request, 'module_name')
        status = assess_status.module_status_dict[module_name].get_assess_status()
        if status['status'] != Macro.ASSESS_PROCESSING:
            return jsonify({'status':0, "Err_info":"the assess in not processing"})

        global_vars.stop_sign[module_name] = True
        return jsonify({'status':Macro.STATUS_SUCCESS})
    except Exception as e:
        return jsonify({'status':Macro.STATUS_FAIL,'Err_info':e.args[0]})
