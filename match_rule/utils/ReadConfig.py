#encoding=utf-8

import configparser
#配置描述
#Crontab round
#    interval 时间间隔
#    limit_num 首次分片数量
#    forward_time 往前N时间段获取数据，防止数据不完整
#    data_interval 数据间隔时间
#
#
#DB
#    db_type    数据库类型
#    db_host    地址
#    db_port    端口
#    db_user    用户名
#    db_passwd    密码
#    db_name   初始连接库


class ReadConfig(object):
    #ConfigParser instance
    cf = configparser.ConfigParser()
    
    #Config file path 
    config_path = '../config/compact.conf'
    
    #Crontab
    interval = 60  #秒
    limit_num = 10000 
    data_interval = 3600  #秒
    forward_time  = 300   #秒  


    #DB
    db_type = 'mysql'    #数据库类型
    db_host = '192.168.35.126'
    db_port = '3306'
    db_user = 'root'
    db_passwd = ''
    db_name = ''
    db_charset = 'utf8'
    db_insertInterval = 0    #模型入库间隔时间
    db_verStartTime = '08'   #入库版本开始时间
    

    
    def __init__(self, config_path):
        #初始配置
        self.config_path = config_path
        self.LoadConfig(config_path)#获取数据库信息

    #载入配置
    def LoadConfig(self, conf_path):
        if self.config_path != conf_path :
            self.config_path = conf_path
        
        self.cf.read(conf_path)


        #DB
        db_sec = self.cf['db']
        self.db_type = db_sec.get("type","mysql")
        self.db_host = db_sec.get('host')
        self.db_port = db_sec.get('port','3306')
        self.db_user = db_sec.get('user')
        self.db_passwd = db_sec.get('passwd')
        self.db_name = db_sec.get('dbname','')
        self.db_charset = db_sec.get('charset','utf8')
        self.db_insertInterval = db_sec.get('insertInterval')
        self.db_verStartTime = db_sec.get('verStartTime')

    #重载配置文件
    def ReloadConfig(self):
        self.LoadConfig(self.config_path)
        
    
    #输出当前所有配置
#    def PrintConfig(self):
#        #Config file path 
#        print('configuration file path : ', self.config_path)
#
#
#        #DB
#        print('===DB===')
#        print('Database type is [%s]' % self.db_type)
#        print('  host = [%s]' % self.db_host)
#        print('  port = [%s]' % self.db_port)
#        print('  user = [%s]' % self.db_user)
#        print('  password = [%s]' % self.db_passwd)
#        print('  dbname = [%s]' % self.db_name)  
#        print('  charset = [%s]' % self.db_charset)

    #设置指定配置
#    def SetSKV(self, secs, opt, kvs):
#        if self.cf is not None:
#            self.cf.set(secs, opt, kvs)
#            self.cf.write(open(self.config_path, 'w'))  
    
    
#def test_func(cfg):
#    print(cfg.db_type)
#        
#if __name__ == '__main__':
#    alm_cfg= ReadConfig('../config/compact.conf')
#    alm_cfg.PrintConfig()
#    print(alm_cfg.interval)
#    test_func(alm_cfg)
#    
    
    