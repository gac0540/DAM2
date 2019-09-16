from flask import Blueprint,render_template,request,flash,redirect,url_for,jsonify
from flask_login import login_user,current_user,login_required,logout_user
from api import g_query_users, base_tools
from werkzeug.security import generate_password_hash, check_password_hash
from extensions import db
from models import user,module,jurisdiction
from settings import Macro


auth_bp = Blueprint('auth',__name__)


@auth_bp.route('/login',methods=['POST'])
def login():
    try:
        user_id, password = base_tools.parse_request_json(request, "user_id","password")
        ret = g_query_users(user_id)
        if ret is not None and check_password_hash(ret.hashed_password,password):
            login_user(ret)
            return jsonify({'status':Macro.LOGIN_STATUS_SUCCESS,
                            'user_role':ret.user_role,
                            'user_id':ret.user_id})
        else:
            return jsonify({'status':Macro.LOGIN_STATUS_FAIL})
    except Exception as e:
        return jsonify({'status':Macro.STATUS_FAIL,"Err_info":e.args})

@auth_bp.route('/logout')
@login_required
def logout():
    logout_user()
    return jsonify({'status':Macro.LOGOUT_STATUS_SUCESS})

@auth_bp.route('/queryuser/<user_id>')
@login_required
def query_user(user_id):
    ret = g_query_users(user_id)
    if ret is not None:
        return jsonify({'status':1})
    else:
        return jsonify({'status': 0})

