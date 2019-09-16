from flask import Flask, url_for, redirect, request, flash, render_template,jsonify
from blueprints.auth import auth_bp
from blueprints.user import user_bp
from blueprints.module import module_bp
from blueprints.data_dict import data_dict_bp
from blueprints.quality import quality_bp
from blueprints.report import report_bp
from blueprints.test import test_bp
from settings import global_vars, assess_status
from extensions import db, login_manager, cors
from models import user, module, jurisdiction
import click
from werkzeug.security import generate_password_hash

def get_module_name_list():
    with app.app_context():
        qret = module.query.all()
        module_name_list = []
        for obj in qret:
            module_name_list.append(obj.module_name)
        return module_name_list

def create_app():
    app = Flask('DAM')
    app.register_blueprint(auth_bp,url_prefix='/auth')
    app.register_blueprint(user_bp, url_prefix='/user')
    app.register_blueprint(module_bp, url_prefix='/module')
    app.register_blueprint(data_dict_bp, url_prefix='/dd')
    app.register_blueprint(test_bp,url_prefix='/test')
    app.register_blueprint(quality_bp, url_prefix='/quality')
    app.register_blueprint(report_bp, url_prefix='/report')

    app.config['SQLALCHEMY_DATABASE_URI'] = global_vars.linkword_mysql
    app.secret_key = "ACMilan1899"

    cors.init_app(app, resources=r'/*', supports_credentials=True)

    login_manager.init_app(app)
    login_manager.login_view = 'auth.login'
    login_manager.login_message = 'Unauthorized User'
    login_manager.login_message_category = "info"

    db.init_app(app)
    return app




app = create_app()
assess_status.init(get_module_name_list())
global_vars.init_stop_sign(global_vars.stop_sign,get_module_name_list())

if __name__ == '__main__':
    app.run()


#回调函数，login-manager根据一个user_id获取到对应user的对象
@login_manager.user_loader
def load_user(user_id):
    return user.query.get(user_id)

@app.cli.command()
def initdb():
    db.drop_all()
    db.create_all()
    user1 = user(user_id='sa', user_name='', user_role= 0, hashed_password=generate_password_hash('123456'))
    user2 = user(user_id='ma', user_name='', user_role= 5, hashed_password=generate_password_hash('123456'))
    user3 = user(user_id='guest', user_name='', user_role=10, hashed_password=generate_password_hash('123456'))
    module1 = module(module_name='pms',database_type='oracle',user_name='C##SCYW',password='SCYW',\
                     ip_address='219.216.69.63',port="1521",sid='orcl')
    module2 = module(module_name='erp', database_type='mysql', user_name='root', password='0000',
                     ip_address='localhost', port="3306", db_name='dam2')
    jd1 = jurisdiction(user_id='sa',module_id=1,jd_code=1)
    jd2 = jurisdiction(user_id='sa', module_id=2, jd_code=1)
    db.session.add(user1)
    db.session.add(user2)
    db.session.add(user3)
    db.session.add(module1)
    db.session.add(module2)
    db.session.add(jd1)
    db.session.add(jd2)
    db.session.commit()
    sql = 'drop table if exists  integrity_result;'
    sql += 'drop table if exists  integrity_result_store;'
    sql += 'drop table if exists redundancy_result;'
    sql += 'drop table if exists redundancy_result_store;'
    db.session.execute(sql)

    sql = 'create table integrity_result\
    (\
    id int auto_increment primary key,\
    module_id int(11),\
    module_name varchar(100),\
    table_name varchar(100),\
    num_rows int(20),\
    num_cols  int(20),\
    num_grids  int(20),\
    num_null_grids  int(20),\
    num_null_cols  int(20),\
    num_full_cols  int(20),\
    num_non_full_cols  int(20),\
    num_non_full_rows  int(20),\
    num_full_rows  int(20),\
    rate_null_cols float,\
    rate_full_rows float,\
    rate_null_grids float,\
    rate_null_grids2 float\
    )'
    db.session.execute(sql)
    sql = 'create table integrity_result_store (id int not null auto_increment primary key,\
    result_time datetime,\
    module_id int(11),\
    module_name varchar(100),\
    table_name varchar(100),\
    num_rows int(20),\
    num_cols  int(20),\
    num_grids  int(20),\
    num_null_grids  int(20),\
    num_null_cols  int(20),\
    num_full_cols  int(20),\
    num_non_full_cols  int(20),\
    num_non_full_rows  int(20),\
    num_full_rows  int(20),\
    rate_null_cols float,\
    rate_full_rows float,\
    rate_null_grids float,\
    rate_null_grids2 float)'
    db.session.execute(sql)
    sql = 'create table redundancy_result\
    (\
    id int(20) PRIMARY KEY AUTO_INCREMENT,\
    module_id int(20)   NOT NULL,\
    module_name varchar(60) NOT NULL,\
    table_a varchar(60) NOT NULL,\
    table_b varchar(60) NOT NULL,\
    num_cols_a int NOT NULL,\
    num_cols_b int NOT NULL,\
    num_same_cols int NOT NULL,\
    len_col_names_a int NOT NULL,\
    len_col_names_b int NOT NULL,\
    lv_distance float NOT NULL,\
    rate_a float NOT NULL,\
    rate_b float NOT NULL\
    )'
    db.session.execute(sql)

    sql = 'create table redundancy_result_store\
    (\
    id int(20) PRIMARY KEY AUTO_INCREMENT,\
    result_time varchar(60) NOT NULL,\
    module_id int(20)   NOT NULL,\
    module_name varchar(60) NOT NULL,\
    table_a varchar(60) NOT NULL,\
    table_b varchar(60) NOT NULL,\
    num_cols_a int NOT NULL,\
    num_cols_b int NOT NULL,\
    num_same_cols int NOT NULL,\
    len_col_names_a int NOT NULL,\
    len_col_names_b int NOT NULL,\
    lv_distance float NOT NULL,\
    rate_a float NOT NULL,\
    rate_b float NOT NULL\
    )'
    db.session.execute(sql)

    sql = 'create table consistency_result\
        (\
        id int(50) PRIMARY KEY AUTO_INCREMENT,\
        module_id int(20)   NOT NULL,\
        module_name varchar(60) NOT NULL,\
        table_A varchar(60) NOT NULL,\
        col_A varchar(60) NOT NULL,\
        table_B varchar(60) NOT NULL,\
        col_B varchar(60) NOT NULL,\
        found_rate float(20) NOT NULL,\
        data_type varchar(60) NOT NULL,\
        avg_col_lenA int(20) NOT NULL,\
        avg_col_lenB int(10) NOT NULL,\
        uuid int(5) NOT NULL\
        )'
    db.session.execute(sql)

    sql = 'create table consistency_result_store\
        (\
        id int(50) PRIMARY KEY AUTO_INCREMENT,\
        result_time varchar(255) NOT NULL,\
        module_id int(20)   NOT NULL,\
        module_name varchar(60) NOT NULL,\
        table_A varchar(60) NOT NULL,\
        col_A varchar(60) NOT NULL,\
        table_B varchar(60) NOT NULL,\
        col_B varchar(60) NOT NULL,\
        found_rate float(20) NOT NULL,\
        data_type varchar(60) NOT NULL,\
        avg_col_lenA int(10) NOT NULL,\
        avg_col_lenB int(10) NOT NULL,\
        uuid int(5) NOT NULL\
        )'
    db.session.execute(sql)



    click.echo('init database')


