from flask import Blueprint,render_template,request,flash,redirect,url_for,jsonify
from api import base_tools, g_get_module_list,  g_get_module_data, g_get_table_data
from models import user

from flask_login import login_user,current_user,login_required,logout_user
from api import g_query_users
from werkzeug.security import generate_password_hash, check_password_hash
from extensions import db
from models import user,module,jurisdiction
from settings import Macro

data_dict_bp = Blueprint('dd',__name__)

@data_dict_bp.route('/datasourcelist',methods=['GET'])
@login_required
def get_data_module_list():
    return g_get_module_list()



@data_dict_bp.route('/getmodule/<module_name>',methods=['GET'])
@login_required
def get_data_module(module_name):
    ret = g_get_module_data(module_name)
    return ret

@data_dict_bp.route('/gettable/<module_name>/<table_name>',methods=['GET'])
@login_required
def get_data_table(module_name,table_name):
    ret = g_get_table_data(module_name, table_name)
    return ret




