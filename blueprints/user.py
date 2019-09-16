from flask import Blueprint,render_template,request,flash,redirect,url_for,jsonify
from api import base_tools
from models import user
from operator import and_

from flask_login import login_user,current_user,login_required,logout_user
from api import g_query_users, g_get_user_list, g_new_user, g_edit_user, g_get_user, g_reset_password
from werkzeug.security import generate_password_hash, check_password_hash
from extensions import db
from models import user,module,jurisdiction
from settings import Macro

user_bp = Blueprint('user',__name__)

@user_bp.route('/list',methods=['GET'])
@login_required
def get_user_list():
    return jsonify({'user_list':g_get_user_list()})

@user_bp.route('/create',methods=['POST'])
@login_required
def new_user():
    data = request.json
    qret = g_query_users(data['user_id'])
    if qret is not None:
        return jsonify({'status':Macro.NEW_USER_FAIL,'ErrCode':Macro.NEW_USER_FAIL_ID_EXISTED})
    try:
        g_new_user(data)
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        return jsonify({'status':Macro.NEW_USER_FAIL,'ErrCode':e.args[0]})
    return jsonify({'status': Macro.NEW_USER_SUCCESS})

@user_bp.route('/delete',methods=['POST'])
@login_required
def delete_user():
    data = request.json
    qret = g_query_users(data['user_id'])
    if  qret is None:
        return jsonify({'status':Macro.DEL_USER_FAIL})
    try:
        db.session.delete(qret)
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        return jsonify({'status':Macro.DEL_USER_FAIL})
    return jsonify({'status':Macro.DEL_USER_SUCCESS})

@user_bp.route('/update',methods=['POST','PUT'])
@login_required
def edit_user():
    data = request.json
    qret = g_query_users(data['user_id'])
    if  qret is None:
        return jsonify({'status':Macro.EDIT_USER_FAIL})
    try:
        g_edit_user(data)
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        return jsonify({'status':Macro.EDIT_USER_FAIL,'Err_info':e.args[0]})
    return jsonify({'status':Macro.EDIT_USER_SUCCESS})

@user_bp.route('/resetpwd',methods=['POST'])
@login_required
def reset_pwd():
    data = request.json
    qret = g_query_users(data['user_id'])
    if qret is None:
        return jsonify({'status':Macro.NEW_USER_FAIL,'ErrCode':"user does not exist"})
    try:
        g_reset_password(data)
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        return jsonify({'status':Macro.NEW_USER_FAIL,'ErrCode':e.args[0]})
    return jsonify({'status': Macro.NEW_USER_SUCCESS})

@user_bp.route('/<user_id>')
@login_required
def query_user_info(user_id):
    ret = g_query_users(user_id)
    if ret is not None:
        return jsonify({'status':1,'user_info':g_get_user(ret)})
    else:
        return jsonify({'status': 0})