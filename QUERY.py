from datetime import datetime, date, timedelta
import pandas as pd
import psycopg2
import re
import copy

pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)



class Query:


    def __init__(self,Month,Project,Business,Post,Indicator,ICN=None,sheetA=None):
        self.Month = Month
        self.Project = Project
        self.Business =Business
        self.Post = Post
        self.Indicator = Indicator
        self.ICN = ICN
        self.con = psycopg2.connect(database="scty", user="duchunjiang", password="duchunjiang", host="192.168.10.59", port="5432")
    
    def format_scheme(self):
        sheet = pd.read_excel(r"D:\anaconda3\Lib\site-packages\DiaoChan\attachment\指标计算逻辑.xlsx",sheet_name="绩效方案")
        sheet = sheet.dropna(axis = 0,how = 'all')
        scheme = sheet.where(sheet.notnull(),"禅")  # 把np.nan替换掉
        # sheet = sheet.apply(lambda x: x.replace(r"\s","",regex=True))
        # sheet = sheet.apply(lambda x: x.replace(r"（","(",regex=True))
        # sheet = sheet.apply(lambda x: x.replace(r"）",")",regex=True))
        # sheet = sheet.apply(lambda x: x.replace(r"！","!",regex=True))
        # sheet = sheet.apply(lambda x: x.replace(r"([^=!<>]+)(=)(\d+)$",r"\1\2\2\3",regex=True))
        # sheet = sheet.apply(lambda x: x.replace(r"^([^=]*)([!<>]=)((?:[^=!<>\d]+[\d]*)|(?:[\d]*[^=!<>\d]+))$",r"{}\1{}\2{}\3{}".format("'","'","'","'"),regex=True))
        # sheet = sheet.apply(lambda x: x.replace(r"([^=!<>]+)(=)((?:[^=!<>\d]+[\d]*)|(?:[\d]*[^=!<>\d]+))$",r"{}\1{}\2\2{}\3{}".format("'","'","'","'"),regex=True))
        # scheme = sheet.apply(lambda x: x.replace(r"(.*?)(and|or)(.*>)",r'\1 \2 \3',regex=True))
        return scheme

    def get_path(self):

        # 人总计
        rzj = pd.read_sql(sql="SELECT * FROM 人总记 LIMIT(3)",con=self.con)
        rzj_head =  list(rzj)
        rzj_head = pd.DataFrame({"指标":rzj_head,"列名":rzj_head,"表名":"人总记"})

        # 人岗计
        rgj = pd.read_sql(sql="SELECT * FROM 人岗记 LIMIT(3)",con=self.con) 
        rgj_head =  list(rgj)
        rgj_head = pd.DataFrame({"指标":rgj_head,"列名":rgj_head,"表名":"人岗记"})

        # # 收入月记1 - 本表中取自身值的
        if self.Business != "全部业务":
            xmsryj = pd.read_sql(sql="SELECT * FROM 项目收入月记 where 月份='{}' and 项目编码='{}' and 业务编码='{}'".format(self.Month,self.Project,self.Business),con=self.con) 
        else:
            xmsryj = pd.read_sql(sql="SELECT * FROM 项目收入月记 where 月份='{}' and 项目编码='{}'".format(self.Month,self.Project),con=self.con) 
        xmsryj = xmsryj if len(xmsryj.index) != 0 else print("【美丽的错误】项目收入月记中没有 '{} {} {}' 相关信息！".format(self.Month,self.Project,self.Business))
        xmsryj_a = list(xmsryj.loc[:,"席位数":"系数计算方式"])
        xmsryj_a = pd.DataFrame({"指标":xmsryj_a,"列名":xmsryj_a,"表名":"项目收入月记"})

        # 收入月记2 - 本表中取隔壁值的
        xmsryj_T = xmsryj.T.reset_index()
        xmsryj_T.columns = ["列名","指标"]
        xmsryj_b = xmsryj_T.loc[xmsryj_T["列名"].str.contains(r"指标[25]\d+名称")].dropna()
        xmsryj_b = xmsryj_b.apply(lambda x: x.replace(r"(指标\d+)名称",r'\1值',regex=True))
        xmsryj_b["表名"] = "项目收入月记"

        # 收入月记3 - 跨表取值的,人日月记都有
        xmsryj_T = xmsryj.T.reset_index()
        xmsryj_T.columns = ["列名","指标"]
        xmsryj_c = xmsryj_T.loc[xmsryj_T["列名"].str.contains(r"指标[134]\d+名称")].dropna()
        xmsryj_c = xmsryj_c.apply(lambda x: x.replace(r"(指标\d+)名称",r'\1',regex=True))
        # 收入月记3.1表名为人日记
        xmsryj_c_rj = copy.deepcopy(xmsryj_c)
        xmsryj_c_rj["表名"] = self.Project
        # 收入月记3.2表名为人月记
        xmsryj_c_yj = copy.deepcopy(xmsryj_c)
        xmsryj_c_yj["表名"] = "人月记"

        # 获取人日记独有指标
        rrj = pd.read_sql(sql="SELECT * FROM {} limit(3)".format(self.Project),con=self.con)
        rrj_head = list(rrj.loc[:,"直属上级姓名":"结算量"])
        rrj_head = pd.DataFrame({"指标":rrj_head,"列名":rrj_head,"表名":self.Project})

        # 获取人月记独有指标
        ryj = pd.read_sql(sql="SELECT * FROM 人月记 limit(3)",con=self.con)
        ryj_head = list(ryj.loc[:,"岗位名称":"结算量调整"])
        ryj_head = pd.DataFrame({"指标":ryj_head,"列名":ryj_head,"表名":"人月记"})

        # 以上合并路径
        all_path = rzj_head.append(rgj_head).append(xmsryj_a).append(xmsryj_b).append(xmsryj_c_rj).append(xmsryj_c_yj).append(rrj_head).append(ryj_head,ignore_index=True)
        # all_path.to_excel(r"C:\Users\user\Desktop\all_path.xlsx")
        return all_path

    def celect_path(self,all_path,reuse=None):

        # 优先寻找人日记
        rrj = all_path.loc[(all_path["指标"]==self.Indicator)&(all_path["表名"].str.contains("sc00"))]
        ryj = all_path.loc[(all_path["指标"]==self.Indicator)&(all_path["表名"]=="人月记")]
     
        # 判断数据库中有没有这个指标
        if self.Indicator in list(all_path["指标"]):
            # 初次调用
            if reuse is None:
                if len(rrj) != 0:
                    sheet_name = list(rrj["表名"])[0]
                    colum_name = list(rrj["列名"])[0]
                elif len(ryj) != 0:
                    sheet_name = list(ryj["表名"])[0]
                    colum_name = list(ryj["列名"])[0]
                else:
                    sheet_name = list(all_path.loc[all_path["指标"]==self.Indicator,"表名"])[0]
                    colum_name = list(all_path.loc[all_path["指标"]==self.Indicator,"列名"])[0]

            # 二次调用
            if reuse is not None:
                all_path = all_path.loc[~all_path["表名"].str.contains("sc00")]
                ryj = all_path.loc[(all_path["指标"]==self.Indicator)&(all_path["表名"]=="人月记")]
                if len(ryj) != 0:
                    sheet_name = list(ryj["表名"])[0]
                    colum_name = list(ryj["列名"])[0]
                else:
                    sheet_name = list(all_path.loc[all_path["指标"]==self.Indicator,"表名"])[0]
                    colum_name = list(all_path.loc[all_path["指标"]==self.Indicator,"列名"])[0]
            return sheet_name,colum_name

        else:
            print("【美丽的错误】在数据库中不存在 '{}' ！".format(self.Indicator))

    def aggregation_dispose(self):
        # 圈定聚合范围

        # 对聚合方式的处理

        # 结果完善
        pass


    def limit_mode(self,Indicator,scheme_row,all_path,sheet_name=None,colum_name=None):

        # [A]处理 Mode 列的条件
        Mode = scheme_row.loc[0,"Mode"]
        if Mode != "禅":
            Query.aggregation_dipose()

        else:
            # [B]处理 path 列的条件,分2种情况
            path = scheme_row.loc[0,"Path"]
            if path != "禅":
                # [B1]处理自定义sql
                zdy_sql = re.findall(r"SQL.*",str(path),flags=re.I) 
                print('zdy_sql:',zdy_sql)
                if len(zdy_sql) > 0:
                    SQLS = pd.read_excel(r"D:\anaconda3\Lib\site-packages\DiaoChan\attachment\指标计算逻辑.xlsx",sheet_name="SQL语句")
                    sql = SQLS.loc[SQLS["语句编号"]==zdy_sql[0],"语句内容"]
                    print(sql[0])
                    print("【提示】查询模块中自定义sql条件，需根据实际应用情景完善。")

                # [B2]处理自定义文件
                zdy_file = re.findall(r"^([cdefg].*?)\[.*?\]$",str(path),flags=re.I) 
                if len(zdy_file) > 0:
                    zdy_file_sheet = re.findall(r"^[cdefg].*?\[(.*?)\]\[.*?\]$",str(path),flags=re.I) 
                    zdy_file_column = re.findall(r"^[cdefg].*?\[.*?\]\[(.*?)\]$",str(path),flags=re.I) 
                    file = pd.read_excel(zdy_file[0],sheet_name=zdy_file_sheet[0])
                    value = file.loc[file["身份证"]==self.ICN,zdy_file_column]
                    value = value.iloc[0,0] if not value.empty else 0
                    print("【提示】查询模块中自定义文件条件，需根据实际应用情景完善。")
            
            # [C]处理日期范围
            DateRange = scheme_row.loc[0,"DateRange"]
            if DateRange != "禅":
                d1 = re.findall(r"^(.*?)到.*?$",str(DateRange),flags=re.I) 
                d2 = re.findall(r"^.*?到(.*?)$",str(DateRange),flags=re.I) 
                d1 = datetime.strptime(str(d1[0]),"%Y-%m-%d").date()
                d2 = datetime.strptime(str(d2[0]),"%Y-%m-%d").date()
                tj_date = "日期 between '{}' and '{}'".format(d1,d2)
            else:
                month1 = datetime.strptime(str(self.Month),"%Y-%m-%d").date()
                next_month = month1.replace(day=28) + timedelta(days=4) 
                month31 = next_month - timedelta(days=next_month.day)
                tj_date = "日期 between '{}' and '{}'".format(month1,month31)


            # [D]处理数据范围
            if self.Business == "全部业务":
                tj_bm = "and 项目编码 = '{}'".format(self.Project)
            else:
                DataRange = scheme_row.loc[0,"DataRange"]
                if DataRange != "禅" and DataRange != "项目":
                    DataRange = DataRange+"编码"
                    # 依据月份业务身份证找出相应编码
                    sql = "select {} from {} where 日期 = '{}' and 业务编码 = '{}' and 身份证 = '{}' ".format(DataRange,
                            self.Project,self.Month,self.Business,self.ICN)
                    bm = pd.read_sql(sql=sql,con=self.con)
                    bm = bm.iloc[0,0]
                    # 把编码作为查询条件
                    tj_bm = "and (业务编码 = '{}' or 中心编码 = '{}' or 主管编码 = '{}' or 组别编码 = '{}') ".format(bm,bm,bm,bm)
                elif DataRange == "项目":
                    tj_bm = "and 项目编码 = '{}'".format(self.Project)
                else:
                    tj_bm = "and 身份证 = '{}' ".format(self.ICN)
            

            # [E]根据以上2个条件合成并修正sql
            # 1 合成标准语句
            if sheet_name is None and colum_name is None:
                sheet_name,colum_name = Query.celect_path(self,all_path)
            sql = "select sum({}) from {} where {} {}".format(colum_name,sheet_name,tj_date,tj_bm)

            # 2 部分表没有身份证 / 身份证为禅
            noicn = ["项目总记","项目成本月记","项目收入月记","项目日记"]
            if sheet_name in noicn or self.ICN == "禅":
                sql = re.sub(r"^(.*?)and.?身份证.?=(.*?)$",r"\1",sql)

            # 3.部分表日期列名为月份
            evermonth = ["项目收入月记","项目成本月记","人月记"]
            if sheet_name in evermonth :
                sql = re.sub(r"^(.*?)日期(.*?)$",r"\1月份\2",sql)
                
            # # 非人日记要加上项目和业务信息
            # if sheet_name.find("sc00") == -1:
            #     sql = re.sub(r"^(.*?)$",r"\1 and 项目编码='{}' and 业务编码='{}';".format(self.Project,self.Business),sql)

            # 人日记要去掉项目编码
            if sheet_name.find("sc00") != -1:
                sql = re.sub(r"^(.*?)and.项目编码.{2,5}sc\d{4}\'(.*)$",r"\1\2;".format(self.Project,self.Business),sql)
            
            # 如果结果为空，就换个表再查一次(人和月记都有此列，但只有月记才有填数据)
            result = pd.read_sql(sql,con=self.con)
            if result.iloc[0,0] is None and sheet_name != "人月记":
                sheet_name,colum_name = Query.celect_path(self,all_path,reuse=1)
                result = Query.limit_mode(self,Indicator,scheme_row,all_path,sheet_name,colum_name)
            if type(result) != int:
                result = 0 if result.iloc[0,0] is None else result.iloc[0,0]

            return result


    def main(self):
        # 创建水果篮
        fruit_basket={}
        # 定位到默认值
        scheme= Query.format_scheme(self)
        scheme_row = scheme.loc[(scheme["Month"]==self.Month)&(scheme["Business"]==self.Business)&(scheme["Post"]==self.Post)&(scheme["Indicator"]==self.Indicator)]
        scheme_row = scheme_row.reset_index(drop=True)

        if len(scheme_row.index) != 0:
            Default_val = list(scheme_row["Default"])[0]
        else:
            print("【美丽的错误】指标计算逻辑表中没有 '{} {} {} {}' 相关信息！".format(self.Month,self.Business,self.Post,self.Indicator))
        # 有默认值取默认值
        if Default_val != "禅":
            fruit_basket[list(scheme_row["Indicator"])[0]] = list(scheme_row["Default"])[0]
        # 无默认值
        else:
            all_path = Query.get_path(self)
            val = Query.limit_mode(self,Indicator=self.Indicator,scheme_row=scheme_row,all_path=all_path)
            fruit_basket[self.Indicator] = val

        # 处理结束
        # print(fruit_basket)
        self.con.close()
        return fruit_basket



if __name__=="__main__":
    # p = Query(Month="2020-06-01",Project="sc0003",Business="sc000301",Post="客服专员",Indicator="接通量",ICN="510623199202213812")
    p = Query(Month="2020-06-01",Project="sc0003",Business="sc000301",Post="客服专员",Indicator="接通量",ICN="510623199202213812")
    begin = datetime.now()
    print("查询结果：",p.main())
    end = datetime.now()
    print("查询耗时：",end-begin)
 