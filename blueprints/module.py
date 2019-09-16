from flask import Blueprint,render_template,request,flash,redirect,url_for,jsonify
from api import base_tools
from models import user
from operator import and_

from flask_login import login_user,current_user,login_required,logout_user
from api import g_query_users, g_get_user_list, g_new_user, g_edit_user, g_get_module_list,g_new_module,g_edit_module, g_query_module
from werkzeug.security import generate_password_hash, check_password_hash
from extensions import db
from models import user,module,jurisdiction
from settings import Macro, Attributes, assess_status

module_bp = Blueprint('module',__name__)


@module_bp.route('/list',methods=['GET'])
@login_required
def get_module_list():
    try:
        ret = g_get_module_list()
    except Exception as e:
        return jsonify({'status':Macro.STATUS_FAIL, 'Err_info':e.args[0]})
    return jsonify({'status': Macro.STATUS_SUCCESS, 'module_list': ret})


@module_bp.route('/create', methods=['POST'])
@login_required
def new_module():
    data = request.json
    qret = module.query.filter(module.module_name==data['module_name']).first()
    if qret is not None:
        return jsonify({'status':Macro.STATUS_FAIL,'ErrCode':"module exists!"})
    try:
        g_new_module(data)
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        return jsonify({'status':Macro.NEW_USER_FAIL,'ErrCode':e.args[0]})
    return jsonify({'status': Macro.NEW_USER_SUCCESS})


@module_bp.route('/update',methods=['POST','PUT'])
@login_required
def edit_module():
    data = request.json
    try:
        if g_query_module(data['module_name']) is None:
            return jsonify({'status':Macro.STATUS_FAIL,'Err_info':"module does not exist"})

        g_edit_module(data)
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        return jsonify({'status':Macro.EDIT_USER_FAIL,'Err_info':e.args[0]})
    return jsonify({'status':Macro.EDIT_USER_SUCCESS})


@module_bp.route('/delete',methods=['POST'])
@login_required
def delete_module():
    try:
        data = request.json
        module_name = data['module_name']
        qret = module.query.filter(module.module_name==module_name).first()
        if qret is not None:
            module_status = assess_status.get_module_status(module_name)
            if module_status['status'] == 1:
                return jsonify({'status':Macro.STATUS_FAIL,'Err_info':'the module is being assessed. to delete the module, stop the assess'})
            else:
                assess_status.del_module_status(module_name)

        db.session.delete(qret)
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        return jsonify({'status':Macro.STATUS_FAIL, "Err_info":e.args[0]})
    return jsonify({'status':Macro.STATUS_SUCCESS})




@module_bp.route('/<module_name>')
@login_required
def query_user_info(module_name):
    ret = g_query_module(module_name)
    if ret is not None:
        return jsonify({'status':1,'module_info':ret})
    else:
        return jsonify({'status': 0})

