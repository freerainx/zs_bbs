'''
封装一个mysql工具类(需要自己写SQL语句)
功能：mysql数据库操作
步骤：
    1.连接数据库
    2.通过连接对象，获取游标对象
    3.增删改查操作
方法：
    1.查
    2.增删改 commit,rollback
'''

# 先要导入pymysql
import pymysql


class MysqlDb:
    # 初始化方法
    def __init__(self):
        # 初始化方法中调用连接数据库的方法
        self.conn = self.get_conn()
        # 调用获取游标的方法
        self.cursor = self.get_cursor()

    # 连接数据库的方法
    def get_conn(self, **config):
        # **config代表不定长参数
        return pymysql.connect(**config)

    # 获取游标
    def get_cursor(self):
        cursor = self.conn.cursor()
        return cursor

    # 查询sql语句返回的所有数据
    def select_all(self, sql):
        self.cursor.execute(sql)
        return self.cursor.fetchall()

    # 查询sql语句返回的一条数据
    def select_one(self, sql):
        self.cursor.execute(sql)
        return self.cursor.fetchone()

    # 查询sql语句返回的几条数据
    def select_many(self, sql, num):
        self.cursor.execute(sql)
        return self.cursor.fetchmany(num)

    # 增删改除了SQL语句不一样其他都是一样的，都需要提交
    def commit_data(self, sql):
        try:
            # 执行语句
            self.cursor.execute(sql)
            # 提交
            self.conn.commit()
            print("提交成功")
        except Exception as e:
            print("提交出错\n:", e)
            # 如果出错要回滚
            self.conn.rollback()

    # 当对象被销毁时，游标要关闭,连接也要关闭
    # 创建时是先创建连接后创建游标，关闭时是先关闭游标后关闭连接
    def __del__(self):
        self.cursor.close()
        self.conn.close()

