import re
import pandas as pd
import psycopg2
import copy
from datetime import datetime, date, timedelta
from DiaoChan.WorkAge import calculate_work_age

pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)



def get_ab(StartDate,EndDate,level=None,Indicators=None,WorkAge=None,Post=None):
    con = psycopg2.connect(database="scty", user="duchunjiang", password="duchunjiang", host="192.168.10.59", port="5432")

    #[A]时间处理
    month_list = pd.date_range(start=StartDate,end=EndDate,freq="MS")
    month_list = [x.strftime('%F') for x in month_list]
    month_list = "('{}')".format(month_list[0]) if len(month_list) == 1 else tuple(month_list)
    
    # 层级和指标格式转换
    level = level if type(level) == list else [level]
    level_x = "('{}')".format(level[0]) if len(level) == 1 else tuple(level)
    Indicators = Indicators if type(Indicators) == list else [Indicators]
    Post = Post if type(Post) == list else [Post]

    #[B]]人员处理
    with open(r"D:\anaconda3\Lib\site-packages\DiaoChan\beans.txt", "r",encoding="utf-8") as f:  # 打开文件
        sql = f.read()
    sql_2 = "月份 in {} and (项目编码 in {} or 业务编码 in {} or 中心编码 in {} or 主管编码 in {} or 组别编码 in {}  or 身份证 in {})".format(month_list,level_x,level_x,level_x,level_x,level_x,level_x)
    sql = re.sub(pattern=r"请补充条件",repl=sql_2,string=sql,flags=re.S|re.I)
    rrj = pd.read_sql(sql=sql,con=con)
    if rrj.empty:
        print("【参数处理】 没有符合条件的相关数据！")
    del rrj["日期"]
    sheetA = rrj.drop_duplicates()

    #[C]]岗位处理
    # 1.把level中的业务编码以下的编码都转成业务编码
    level_y = list(map(lambda x: re.sub(r"(sc\d{6})\d{0,7}",r'\1',string = x),level))
    level_y = "('{}')".format(level_y[0]) if len(level_y) == 1 else tuple(level_y)
    # 2.查出月记信息
    sql_yj = "select 月份,身份证,岗位名称,业务编码 from 人月记 where 月份 in {}  and (业务编码 in {} or 项目编码 in {} or 身份证 in {})".format(month_list,level_y,level_y,level_y)
    ryj = pd.read_sql(sql=sql_yj,con=con)
    # 3. 月记和sheetA关联合并补充岗位信息
    ryj["月份"] = ryj["月份"].apply(lambda x: datetime.strftime(x,'%Y-%m-%d'))
    sheetA = pd.merge(sheetA,ryj,on=["身份证","业务编码","月份"],how="left")
    # 4. 岗位条件筛选
    sheetA = sheetA if Post[0] is None else sheetA.loc[sheetA["岗位名称"].isin(Post)]

    # [D]工龄处理
    if WorkAge is not None:
        Project_list = set(sheetA["项目编码"])
        sheetA0 = pd.DataFrame()
        for Project in Project_list:
            ag = calculate_work_age(Project)
            ag["月份"] = ag["月份"].apply(lambda x: datetime.strftime(x,'%Y-%m-%d'))
            sheetA1 = sheetA.loc[sheetA["项目编码"]==Project]
            sheetA1 = pd.merge(sheetA1,ag,on=["月份","项目编码","身份证","岗位名称"],how="left")
            sheetA0 = sheetA0.append(sheetA1)
        WorkAge = WorkAge if type(WorkAge) == list else [WorkAge]
        sheetA = sheetA0.loc[sheetA0["工龄"].isin(WorkAge)]


    # [E]指标处理
    IDS = pd.DataFrame()
    for i in Indicators:
        a = copy.deepcopy(sheetA)
        a["指标名称"] = i
        IDS = IDS.append(a)
    sheetA = IDS
    if sheetA.empty:
        print("【参数处理】 没有符合条件的相关数据！")
    
    # -------------------------------------------------------------------------------------------

    # 做B表，业务和岗位均不限，可以作为计算模块的参数，用于计算整体。
    level_s = [x for x in level if len(str(x))==18]
    if len(level_s) != len(level):
        sheetB= sheetA.loc[:,["月份","项目编码","指标名称"]]
        sheetB["业务名称"] = "全部业务"
        sheetB["岗位名称"] = "全部岗位"
        sheetB = sheetB.drop_duplicates()
        # # 与绩效方案匹配取共有
        # fangan = pd.read_excel(r"D:\anaconda3\Lib\site-packages\DiaoChan\attachment\指标计算逻辑.xlsx",sheet_name="sc0004")
        # sheetB = fangan.loc[(fangan["Month"].isin(list(sheetB["月份"])))&(fangan["Project"].isin(list(sheetB["项目编码"])))&(fangan["Business"].isin(list(sheetB["业务名称"])))&\
        #     (fangan["Post"].isin(list(sheetB["岗位名称"])))&(fangan["Indicator"].isin(list(sheetB["指标名称"]))),"Month":"Formula1"]
    else:
        sheetB = sheetA
        # sheetB.rename(columns=('月份': 'Month', '$b': 'b', '$c': 'c', '$d': 'd', '$e': 'e'}, inplace=True) 
    
    sheetA = sheetA.where(sheetA.notnull(),"禅")
    sheetB = sheetB.where(sheetB.notnull(),"禅")
    return sheetA,sheetB



if __name__ == "__main__":

    a = datetime.now()
    sheetA,sheetB = get_ab(StartDate="2020-7-1",EndDate="2020-7-1",level=["510824199610012566"],Indicators=["绩效工资"], WorkAge = ['M3'], Post = ['客服专员'])
    # sheetA.to_excel(r"C:\Users\user\Desktop\新建 XLSX 工作表.xlsx")
    # print(sheetB)

    from DiaoChan.Calculate1 import Calculate
    for index,row in sheetA.iterrows():
        p = Calculate(Month=row["月份"],Project=row["项目编码"],Business=row["业务编码"],Post=row["岗位名称"],Indicator=row["指标名称"],ICN=row["身份证"])
        val = p.main()
        sheetA.loc[index,"指标值"] = val[row["指标名称"]]
    
    b = datetime.now()
    print("耗时：",b-a)
    print(sheetA)


    
