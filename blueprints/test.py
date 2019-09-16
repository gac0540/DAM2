from flask import Blueprint,render_template,request,flash,redirect,url_for,jsonify
from flask_login import login_user,current_user,login_required,logout_user
from api import g_query_users
from werkzeug.security import generate_password_hash, check_password_hash
from extensions import db
from models import user,module,jurisdiction
import random
from algorithms.integrity import integrity
from sqlalchemy import create_engine

test_bp = Blueprint('test',__name__)

@test_bp.route('/testtd/<module_name>')
def testtd(module_name):
    module_info = module.query.filter(module.module_name==module_name).first()
    ck_int = integrity(module_info)
    ck_int.check_integrity()
    return jsonify({'asd':1})

@test_bp.route('/executemany', methods=['POST'])
def test_executemany():
    print("read table_dict from json file")
    import json
    from api import base_tools
    with open("redundancy_result.txt", "r") as f:
        result_list = json.load(f)
    base_tools.write_many_dict_mysql(result_list,"redundancy_result")

    # for result in result_list:
    #     base_tools.write_dict_mysql(result,'redundancy_result')

    return jsonify({'status':'recieved'})


@test_bp.route('/save_table_dict', methods=['POST'])
def save_table_dict():
    import json
    import pickle

    from algorithms.table_dict import prepare_table_dict
    module_info = module.query.filter(module.module_name == 'pms').first()
    table_dict = prepare_table_dict(module_info)

    with open("table_dict.bin", "wb") as f:
        pickle.dump(table_dict,f)

    return jsonify({'status':'recieved'})




@test_bp.route('/t1', methods=['POST'])
def t1():
    return jsonify({'status':'recieved'})

@test_bp.route('/t2', methods=['POST'])
def t2():
    data = request.json
    return jsonify(data)

@test_bp.route('/t3/param', methods=['POST'])
def t3(param):
    return jsonify({'status':param})


@test_bp.route('/login',defaults={'code':-1,'role':-1})
@test_bp.route('/login/<int:code>/<int:role>',methods=['GET','POST'])
def login(code =-1, role =-1):
    statuses = ["fail","success"]
    roles = ['super admin','module admin','guest']
    status_ret = -1
    role_ret = -1
    if code == -1:
        status_ret = random.randint(0,1)
    else:
        status_ret = code
    if role == -1:
        role_ret = random.randint(0,2)
    else:
        role_ret = role
    ret = {
        'status':statuses[status_ret],
        'role':roles[role_ret]
    }


    return ret