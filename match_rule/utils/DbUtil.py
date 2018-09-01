import pymysql as mdb
import sys
import os

sys.path.append("..")
from utils.LightMysql import LightMysql
from utils.ReadConfig import ReadConfig

class DbUtil(object):
	'''
	classdocs
	'''


	def __init__(self):
			'''
			Constructor
			'''
	#返回当前时间或时间戳
	def getDb(self):
		BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
         #返回当前文件的根目录的前一个目录
		config_path = BASE_DIR+'/config/compact.conf'
		self.alm_cfg= ReadConfig(config_path)#输入的是数据库信息，返回的是一个对象所以要用"."
		self.db_config = {'host': self.alm_cfg.db_host,
										'port': int(self.alm_cfg.db_port),
										'user': self.alm_cfg.db_user,
										'passwd': self.alm_cfg.db_passwd,
										'db': self.alm_cfg.db_name,
										'charset':'utf8'}
		print(self.db_config)
		self.db = LightMysql(self.db_config)  # 创建LightMysql对象，若连接超时，会自动重连
         #这个就是连接数据库
         #并且在这个py文件里定义了query函数
         #其形式为：query(self, sql, ret_type='all', values=None):
		return self.db