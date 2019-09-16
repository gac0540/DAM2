from extensions import db
from flask_login import UserMixin

class user(db.Model, UserMixin):
    @property
    def id(self):
        return self.user_id
    user_id = db.Column(db.String(20), primary_key = True)
    user_name = db.Column(db.String(20), nullable = True)
    user_role = db.Column(db.Integer, nullable = False)
    hashed_password = db.Column(db.String(100), nullable = False)
    jurisdiction_s = db.relationship('jurisdiction', passive_deletes = True)


class module(db.Model):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    module_name = db.Column(db.String(100), nullable = False)
    database_type = db.Column(db.String(100), nullable = False)
    user_name = db.Column(db.String(100), nullable = False)
    password = db.Column(db.String(100), nullable = False)
    ip_address = db.Column(db.String(100), nullable = False)
    port = db.Column(db.String(100), nullable = False)
    db_name = db.Column(db.String(100), nullable = True)
    sid = db.Column(db.String(100), nullable = True)
    jurisdiction_s = db.relationship('jurisdiction', passive_deletes = True)


class jurisdiction(db.Model):
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    user_id = db.Column(db.String(20), db.ForeignKey('user.user_id',ondelete="CASCADE"), nullable=False)
    module_id = db.Column(db.Integer, db.ForeignKey('module.id',ondelete="CASCADE"), nullable=False)
    jd_code = db.Column(db.Integer, nullable = False)
