from datetime import datetime, date, timedelta
import pandas as pd
import psycopg2

pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)

def post_and_basic_salary(project_number,month,id_number):
    month1 = datetime.strptime(str(month),"%Y-%m-%d").replace(day=1).date()
    next_month = month1.replace(day=28) + timedelta(days=4) 
    month31 = (next_month - timedelta(days=next_month.day))

    # 获取基数和上岗日期信息
    TOAB = take_office_and_base(month=month,id_number=id_number)
    
    # 当月有上岗记录，采用时段1加时段2.时段1为当月1日到上岗日，基数为之前月基数，若为空取0；计算2为上岗日到31日，基数为当月基数。
    if TOAB["take_office_current"] is not None:
        # 时段1
        if TOAB["take_office_before"] is None:
            base1 = 0
        else:
            WKT = work_time(start_date=month1, end_date=TOAB["take_office_current"] - timedelta(days=1), project_number=project_number,id_number=id_number) # 获取工时
            if WKT["排班工时"] != 0:
                basic_salary1 = TOAB["basic_salary_base_before"]/WKT["排班工时"]*WKT["基本薪资工时"] + TOAB["basic_salary_base_before"]/WKT["排班工时"]*WKT["病假工时"]*0.6
                post_salary1 = TOAB["post_salary_base_before"]/WKT["排班工时"]*WKT["岗位薪资工时"] + TOAB["post_salary_base_before"]/WKT["排班工时"]*WKT["病假工时"]*0.6
                base1 = basic_salary1 + post_salary1
            else:
                base1 = 0
        # 时段2
        
        WKT = work_time(start_date=TOAB["take_office_current"],end_date=month31, project_number=project_number,id_number=id_number) #获取工时
        if WKT["排班工时"] != 0:
            basic_salary2 = TOAB["basic_salary_base_current"]/WKT["排班工时"]*WKT["基本薪资工时"] + TOAB["basic_salary_base_current"]/WKT["排班工时"]*WKT["病假工时"]*0.6
            post_salary2 =  TOAB["post_salary_base_current"]/WKT["排班工时"]*WKT["岗位薪资工时"] + TOAB["post_salary_base_current"]/WKT["排班工时"]*WKT["病假工时"]*0.6
            base2 = basic_salary2 + post_salary2
        else:
            base2 = 0
        # 合计
        base = base1 + base2

    # 当月没有上岗记录，且之前月份有上岗记录，时段为当月全月，基数为之前月基数。
    elif (TOAB["take_office_current"] is None) and (TOAB["take_office_before"] is not None) :
            WKT = work_time(start_date=month1, end_date=month31, project_number=project_number,id_number=id_number)  # 获取工时
            if WKT["排班工时"] != 0:
                basic_salary = TOAB["basic_salary_base_before"]/WKT["排班工时"]*WKT["基本薪资工时"] + TOAB["basic_salary_base_before"]/WKT["排班工时"]*WKT["病假工时"]*0.6
                post_salary =  TOAB["post_salary_base_before"]/WKT["排班工时"]*WKT["岗位薪资工时"] + TOAB["post_salary_base_before"]/WKT["排班工时"]*WKT["病假工时"]*0.6
                base = basic_salary + post_salary
            else:
                base = 0

    # 其他情况均为0
    else:
        base = 0
    return base


if __name__ =='__main__':
    bs = post_and_basic_salary("sc0004","2020-6-1","511102199610011226")
    print(bs)
