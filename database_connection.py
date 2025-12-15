import mysql.connector
from configparser import ConfigParser

config = ConfigParser()
config.read('configuration.ini')
host = config.get('database', 'host')
user = config.get('database', 'user')
passwd = config.get('database', 'passwd')

def get_connection():
    try:
        return mysql.connector.connect(host=host,user=user,passwd=passwd)
    except NameError as e:
        return {
            'message':'something wrong in credientials',
        }
    except Exception as e:
        return {
            'message':'something went wrong in python code',
        }
# cursor = connection.cursor()
# # cursor.execute('insert into test.customers values (%s,%s,%s)',('11','sariq khan','india'))
# cursor.execute('select * from test.customers')
# for row in cursor:
#     print(row)
# connection.commit()
# connection.close()
# cursor.close()

print(get_connection())



print('good bye phir nahi milange')









