import prestodb
import pandas as pd
# 秘鲁的presto
# IP：110.238.85.122
# 10.211.2.91
# 端口：5797
# 账户：duanzhicheng200506
# 密码：无
# mysql_peru.peru.audit_into_pieces
class PrestoDBUtil:
    def get_connect(self):
        conn=prestodb.dbapi.connect(
            host='10.211.2.91',
            port=5797,
            user='duanzhicheng200506',
            catalog='mysql_peru',
            # schema='default',
            schema='peru',
            http_scheme='http')
        return conn

    def execute_select(self, sql):
        conn = self.get_connect()
        cursor = conn.cursor()
        cursor.execute(sql)
        result = []
        try:
            result = cursor.fetchall()
            columns_names = [col[0] for col in cursor.description]
        except Exception as e:
            print(columns_names)
            print(e)
        cursor.close()
        conn.close()
        return result
        return result



if __name__ == '__main__':
    pdu = PrestoDBUtil()
    # sql = 'show tableS'
    sql = '''
        select order_number,user_id,reg_phone,user_identity_code1 ,user_name ,birth_date ,birthplace ,gender,id_image1,id_image2,face_image
        from mysql_peru.peru.audit_into_pieces 
        '''
    res = pdu.execute_select(sql)

    df = pd.DataFrame(res, columns=['order_number', 'user_id', 'reg_phone', 'user_identity_code1', 'user_name',
                                    'birth_date', 'birthplace', 'gender', 'id_image1', 'id_image2', 'face_image'])

    # 保存为 CSV 文件
    df.to_csv('result.csv', index=False)


