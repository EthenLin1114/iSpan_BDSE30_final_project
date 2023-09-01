from flask import Flask

'''
Flask 初始化
'''
app = Flask(__name__, template_folder='templates',
            static_folder="****", static_url_path="/****")
app.static_folder = 'static'

from app import views
