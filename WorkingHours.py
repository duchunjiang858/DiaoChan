from datetime import datetime, date, timedelta
import pandas as pd
import psycopg2



# 目前只支持一个月内的时间范围输入
def work_time(project_number,id_number,start_date,end_date):

    con = psycopg2.connect(database="scty", user="duchunjiang", password="duchunjiang", host="192.168.10.59", port="5432")

    #【满勤排班工时】
    month = datetime.strptime(str(start_date),"%Y-%m-%d").replace(day=1).date()
    manqing_sql = "select 满勤工时 from 人月记 where 项目编码 = '{}' and 身份证='{}' and 月份 = '{}'".format(project_number,id_number,str(month))
    manqing = pd.read_sql(sql=manqing_sql,con=con)
    if not manqing.empty:
        plan_hours = manqing["满勤工时"][0]
    else:
        plan_hours = 0

    #【病假、基本工资和岗位工资的工时】
    sql = "select 日期,排班时长,出勤状态,缺勤时长 from {} where 身份证='{}' and 日期 between '{}' and '{}' order by 日期 asc".format(project_number,id_number,str(start_date),str(end_date))
    attendance = pd.read_sql(sql=sql,con=con) 
    status_list1 = ["陪产假","丧假","婚假","正常","产假"]  # 正常算基本工资
    status_list2 = ["陪产假","丧假","婚假","产检假","正常"]  # 正常算岗位工资
    a = ["事假","病假","其它假"]  # 出勤状态为不算工资的假种，使用排班时长减缺勤时长计算正常出勤工时。

    sick_leave = attendance.loc[attendance["出勤状态"] == "病假"]["缺勤时长"].sum()
    attend1 = attendance.loc[attendance["出勤状态"].isin(status_list1)]["排班时长"].sum() + attendance.loc[attendance["出勤状态"].isin(a)]["排班时长"].sum() - attendance.loc[attendance["出勤状态"].isin(a)]["缺勤时长"].sum()
    attend2 = attendance.loc[attendance["出勤状态"].isin(status_list2)]["排班时长"].sum() + attendance.loc[attendance["出勤状态"].isin(a)]["排班时长"].sum() - attendance.loc[attendance["出勤状态"].isin(a)]["缺勤时长"].sum()
    
    # 【实出勤，不算岗前培训】
    attendance1 = attendance.loc[attendance["出勤状态"] != "岗前培训"]
    actual_attend = attendance1["排班时长"].sum() - attendance1["缺勤时长"].sum()

    # 岗中培训状态中需要计算为正常的工时，如培训4h,另外4h为正常出勤。
    gzc = attendance.loc[attendance["出勤状态"]=="岗中培训"]["排班时长"].sum() - attendance.loc[attendance["出勤状态"]=="岗中培训"]["缺勤时长"].sum()

    # 滴滴项目休息也算实出勤
    if project_number == "sc0003" or project_number == "sc0004":
        didi = attendance.loc[attendance["出勤状态"]=="休息"].shape[0] * 8
    else:
        didi = 0

    print(actual_attend,didi,gzc)

    WKT = {}
    WKT["排班工时"] = plan_hours
    WKT["病假工时"] = sick_leave
    WKT["出勤工时"] = actual_attend + didi + gzc
    WKT["基本薪资工时"] = attend1 + didi + gzc
    WKT["岗位薪资工时"] = attend2 + didi + gzc

    return WKT


if __name__ =='__main__':
    WKT = work_time(project_number="sc0004",id_number="510113199105126215",start_date="2020-4-1",end_date="2020-4-30")
    print(WKT)
