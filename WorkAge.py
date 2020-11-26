
from datetime import datetime, date, timedelta
import pandas as pd
import psycopg2



# 计算工龄
def calculate_work_age(Project):

    # # 代码计算法---------------------------------------------------------------------------

    # '''
    # 用法：
    # 1.输入一个身份证获取单人工龄，输入身份证列表获取列表中对应人员工龄并以DateFrame返回。
    # 2.月份为

    # 算法1：工龄 = 所计算月的年数*12 + 所计算月的月数 - 上岗日期的年数*12 - 上岗日期的月数 + （if 上岗日期天数 <=15 ,1 ,0）
    # 算法2：多个上岗日期选择最早的一个??????
    # 算法3：二次入职的应选二次入职后的第一个岗位上岗日期。（后续实现）

    # '''

    # # 数据库链接查询上岗日期
    # con = psycopg2.connect(database="scty", user="huzhanyuan", password="huzhanyuan", host="192.168.10.59", port="5432")
    # select1 = "select distinct 上岗日期,姓名 from 人岗记 where 身份证='{}'".format(id_number)
    # sql_result1 = pd.read_sql(select1,con=con)   
    # take_office = sql_result1['上岗日期'].min()    

    # # 上岗日期非空
    # if not sql_result1.empty:
    #     month = datetime.strptime(str(month),"%Y-%m-%d")
    #     another_month = 1 if take_office.day <= 15 else 0
    #     num = month.year * 12 + month.month - take_office.year * 12 - take_office.month + another_month
    #     if num < 0 :
    #         work_age = "#N/A"
    #         print("【计算工龄】当前月份早于员工最早的上岗月份!")
    #     elif num < 7 :
    #         work_age = "M"+str(num)
    #     else:
    #         work_age = "M6以上"
    #     # print("{}  最早上岗日期为{}  截至该月份工龄为：{}".format(sql_result1['姓名'][0],take_office,work_age))
    # else:
    #     work_age = "M0"
    # return work_age

    # sql查询法---------------------------------------------------------------------------
    sql = "SELECT S.月份,S.项目编码,S.身份证,S.岗位名称,\
           CASE WHEN (( EXTRACT ( YEAR FROM 月份 ) - EXTRACT ( YEAR FROM DATE ) ) * 12+EXTRACT ( MONTH FROM 月份 )\
                - EXTRACT ( MONTH FROM DATE ) + ( CASE WHEN EXTRACT ( DAY FROM DATE ) <= 15 THEN 1 ELSE 0 END ) ) = 0 THEN 'M0'\
                WHEN (( EXTRACT ( YEAR FROM 月份) - EXTRACT ( YEAR FROM DATE ) ) * 12+EXTRACT ( MONTH FROM 月份 )\
                    - EXTRACT ( MONTH FROM DATE ) + ( CASE WHEN EXTRACT  ( DAY FROM DATE ) <= 15 THEN 1 ELSE 0 END ) ) = 1 THEN 'M1'\
                WHEN (( EXTRACT ( YEAR FROM 月份 ) - EXTRACT ( YEAR FROM DATE ) ) * 12+EXTRACT ( MONTH FROM 月份 )\
                    - EXTRACT ( MONTH FROM DATE ) + ( CASE WHEN EXTRACT ( DAY FROM DATE ) <= 15 THEN 1 ELSE 0 END ) ) = 2 THEN 'M2'\
                WHEN (( EXTRACT ( YEAR FROM 月份 ) - EXTRACT ( YEAR FROM DATE ) ) * 12+EXTRACT ( MONTH FROM 月份 )\
                    - EXTRACT ( MONTH FROM DATE ) + ( CASE WHEN EXTRACT ( DAY FROM DATE ) <= 15 THEN 1 ELSE 0 END ) ) = 3 THEN 'M3'\
                ELSE'M3以上'\
                END AS 工龄\
            FROM\
                (SELECT\
                A.月份,A.身份证,A.项目编码,A.业务编码, A.DATE,A.岗位名称\
                FROM\
                    (SELECT P.月份,Q.DATE,P.身份证,P.项目编码,P.业务编码,Q.岗位名称\
                    FROM\
                        (SELECT 月份,身份证,项目编码,业务编码 FROM 人月记 WHERE 项目编码='{}') AS P\
                        INNER JOIN\
                        (SELECT C.身份证,C.项目编码,C.业务编码,C.岗位名称,C.DATE,C.离岗日期\
                    FROM( SELECT  B.身份证,B.项目编码,B.业务编码,B.岗位名称,B.DATE,SP.离岗日期\
                    FROM(SELECT 身份证,项目编码,业务编码,岗位名称,MIN(上岗日期) AS DATE\
                    FROM 人岗记 WHERE  项目编码='{}' GROUP BY 身份证,项目编码,业务编码,岗位名称 ) AS B INNER JOIN 人岗记 AS SP ON B.身份证=SP.身份证\
                        AND B.项目编码=SP.项目编码 AND B.业务编码=SP.业务编码 WHERE B.项目编码='{}'\
                ) AS C GROUP BY C.身份证,C.项目编码,C.业务编码,C.岗位名称,C.DATE,C.离岗日期) AS Q ON P.身份证 = Q.身份证 AND P.业务编码=Q.业务编码 WHERE Q.项目编码='{}' AND\
                P.月份 BETWEEN Q.DATE AND COALESCE(Q.离岗日期, '2050-01-01')) AS A\
                GROUP BY A.月份,A.身份证,A.DATE,A.项目编码,A.业务编码,A.岗位名称) AS S\
            ORDER BY S.身份证".format(Project,Project,Project,Project)

    con = psycopg2.connect(database="scty", user="huzhanyuan", password="huzhanyuan", host="192.168.10.59", port="5432")
    result = pd.read_sql(sql,con=con)
    return result


if __name__=='__main__':
    work_age = calculate_work_age(Project="sc0003")
    print(work_age)





