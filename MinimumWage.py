
from datetime import datetime, date, timedelta
import pandas as pd
import psycopg2


'''
规则1：招行账分和E招贷按上岗30自然日计算保护期，其他项目按上岗30工作日计算保护期。（未处理）
规则2：保护期为员工上岗开始出勤状态为正常的240个工时。
'''

pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)

def calculate_lowest_salary(month,project_number,id_number):

    # 数据库链接
    con = psycopg2.connect(database="scty", user="duchunjiang", password="duchunjiang", host="192.168.10.59", port="5432")
    
    # 日期处理
    month = datetime.strptime(str(month),"%Y-%m-%d").date()
    next_month = month.replace(day=28) + timedelta(days=4) 
    month31 = next_month - timedelta(days=next_month.day)

    # 查询上岗日期
    select1 = "select min(上岗日期) from 人岗记 where 身份证='{}' and 项目编码 = '{}'".format(id_number,project_number)
    take_office = pd.read_sql(select1,con=con)
    
    if take_office["min"][0] is None:
        money = 0
    else:
        # 上岗日期距离本月太远即为0
        kd = (month31 - take_office["min"][0]).days

        if kd >60:
            money = 0
        else:
            # 查询本月保护工时
            select2 = "select 日期,出勤状态,排班时长,缺勤时长 from {} where 身份证='{}' and 日期 between '{}' and '{}' order by 日期 asc;".format(project_number,id_number,take_office["min"][0],month31)
            working = pd.read_sql(select2,con=con)
            wk30 = working.head(30)
            wk30["日期"] = pd.to_datetime(wk30["日期"])
            wk30 = wk30.loc[(wk30["日期"]>= "{}".format(month))&(wk30["日期"]<= "{}".format(month31))]
            baohugongshi = wk30.sum()["排班时长"]

            # 滴滴休息也算出勤保护
            if project_number == "sc0003" or project_number == "sc0004":
                didi = wk30.loc[wk30["出勤状态"]=="休息"].shape[0] * 8
            else:
                didi = 0

            # money
            money = 1300/240*(baohugongshi + didi)
      
    return round(money,2) 


if __name__ =='__main__':
    money = calculate_lowest_salary(month="2020-5-1",id_number='511002199808152520',project_number="sc0004")
    print(f"该月可用保护金额：{money}元")
