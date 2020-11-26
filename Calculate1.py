import re
import pandas as pd
import psycopg2
import copy
from datetime import datetime, date, timedelta
import math

# pd.set_option('display.unicode.ambiguous_as_wide', True)
# pd.set_option('display.unicode.east_asian_width', True)

class Calculate:

    def __init__(self,Month,Project,Business,Post,Indicator,ICN="禅",gongshi_file=None,scheme=None,yjtq={}):
        self.Month = Month
        self.Project = Project
        self.Business = Business
        self.Post = Post
        self.Indicator = Indicator
        self.ICN = ICN
        self.gongshi_file = pd.DataFrame()
        self.fruit_basket = {}
        self.scheme = scheme
        self.yjtq = yjtq


    def get_scheme(self):
        from DiaoChan.ExcelStyle import beautify
        scheme = beautify()
        scheme = scheme.loc[(scheme["Month"]==self.Month)&(scheme["Project"]==self.Project)&(scheme["Business"]==self.Business)&(scheme["Post"]==self.Post)]
        self.scheme = scheme

    def pick_formula(self,gongshi,scheme):  # 需要公式的归属信息做辅助判断。
        # print("【原始字符】： ",gongshi)
        gongshi_split = re.split(r"[\+\-\*\/<>=(max)(or)(and)\(\)\（\）,!！，]",gongshi)
        # print("【分割字符】： ",gongshi_split)
        gongshi_split = [re.sub(r"\s|\'|\"",repl="",string=x) for x in gongshi_split]
        # print("【二次处理】： ",gongshi_split)
        Indicator_all = scheme.loc[(scheme["Month"]==self.Month)&(scheme["Project"]==self.Project)&(scheme["Business"]==self.Business)&(scheme["Post"]==self.Post),"Indicator"]
        Indicator_all = list(Indicator_all)
        # print("【指标列表】： ",Indicator_all)
        zhibiao_list = [i for i in gongshi_split if i in Indicator_all]
        zhibiao_list = list(set(zhibiao_list))
        # print("【最终结果】： ",zhibiao_list)
        return zhibiao_list

    def calculate_formula(self,scheme,Indicator):
        formula_row = scheme.loc[scheme["Indicator"]==Indicator,"Formula1":"Value15"]
        for i in range(1,16):
            f = "Formula{}".format(i)
            f = formula_row[f]
            f = list(f)[0]

            v = "Value{}".format(i)
            v = formula_row[v]
            v = list(v)[0]
    
            try:
                if f != "禅":
                    if type(eval(f)) is not bool :
                        self.fruit_basket[Indicator] = round(eval(str(f)),2)
                        break

                    elif eval(f):
                        self.fruit_basket[Indicator] = round(eval(str(v)),2)
                        break

            except ZeroDivisionError:
                f = re.sub(r"(.*?/)0(.0)?(.*?)",r"\1庭花0.01\3",f)
                v = re.sub(r"(.*?/)0(.0)?(.*?)",r"\1庭花0.01\3",str(v))
                f = re.sub(r"庭花","",f)
                v = re.sub(r"庭花","",v)

                if f != "禅":
                    if type(eval(f)) is not bool :
                        self.fruit_basket[Indicator] = round(eval(str(f)),2)
                        break

                    elif eval(f):
                        self.fruit_basket[Indicator] = round(eval(str(v)),2)
                        break


    def engine(self,scheme,Indicator):

        # 定位到公式
        gongshi = scheme.loc[scheme["Indicator"]==Indicator,"Formula1":"Value15"]
        gongshi = gongshi.reset_index()
        f_v = str(gongshi.loc[0,"Formula1"])
        for i in range(1,16):
            f_v = f_v  + "," + str(gongshi.loc[0,f"Value{i}"]) + "," + str(gongshi.loc[0,f"Formula{i}"])
        gongshi = gongshi.loc[0,"Formula1"]

        # 解析公式,提取公式中的指标为列表。
        Indicator_list = Calculate.pick_formula(self,gongshi=f_v,scheme=scheme)

        # 情况1：没有公式
        if gongshi == "禅":

            # 1.1查询数据
            from DiaoChan import QUERY
            p = QUERY.Query(Month=self.Month,Project=self.Project,Business=self.Business,Post=self.Post,Indicator=Indicator,ICN=self.ICN)
            p = p.main()
            # 1.2查询结果存入数据池
            self.fruit_basket = {**self.fruit_basket,**p}
            # 1.3判断是否满足输出要求
            keys = list(self.fruit_basket.keys())
            Indi = [self.Indicator]
            # 1.3T 满足输出需要，处理返回计算结果事宜
            if set(Indi) <= set(keys):
                # print("计算完成！")
                pass

            # 1.3F 不满足输出要求，更改方案内容后继续解析
            else:
                fb2 = copy.deepcopy(self.fruit_basket)
                [fb2.pop(k) for k,v in self.yjtq.items()]
                # 1.3F1 把水果篮中的键值都替换到方案中
                for k,v in fb2.items():
                    self.yjtq[k] = v
                    # print("要替换的东西",k,v)
                    # print("替换前",scheme.loc[:,"Month":"Value1"],"\n")
                    scheme.loc[:,"Formula1":"Value15"] = scheme.loc[:,"Formula1":"Value15"].apply(lambda x: x.replace(r"(.*[\+\-\*\/\=\!\(\)\<\>\s\'\"]+)({})([\'\"\+\-\*\/\=\!\(\)\>\<\s]+.*)".format(k),r"\1烟雨{}\3".format(v),regex=True))
                    scheme.loc[:,"Formula1":"Value15"] = scheme.loc[:,"Formula1":"Value15"].apply(lambda x: x.replace(r"^({})([\+\-\*\/\=\!\(\)\>\<\'\"\s]+.*)$".format(k),r"{}\2".format(v),regex=True))
                    scheme.loc[:,"Formula1":"Value15"] = scheme.loc[:,"Formula1":"Value15"].apply(lambda x: x.replace(r"^(.*[\+\-\*\/\=\!\(\)\>\<\'\"\s]+)({})$".format(k),r"\1烟雨{}".format(v),regex=True))
                    scheme.loc[:,"Formula1":"Value15"] = scheme.loc[:,"Formula1":"Value15"].apply(lambda x: x.replace(r"^{}$".format(k),v,regex=True))
                    scheme.loc[:,"Formula1":"Value15"] = scheme.loc[:,"Formula1":"Value15"].apply(lambda x: x.replace("烟雨","",regex=True))
                    # print("替换后",scheme.loc[:,"Month":"Value1"],"\n")

                # 1.3F2 从头开始处理上一级的指标与公式
                # 找出上级指标
                sjzb = self.gongshi_file.loc[self.gongshi_file["指标"]==Indicator,"Indicator"]
                sjzb = list(sjzb)[0]
                # 子调用
                Calculate.engine(self,scheme=self.scheme,Indicator=sjzb)


        # 情况2：有公式，且公式中有指标名称
        if gongshi != "禅" and len(Indicator_list) > 0:

            # 2.1 依次从头处理公式中的每个指标
            # for Indicator2 in Indicator_list:
            Indicator2 = Indicator_list[0]
            # 建立公式档案
            fa = scheme.loc[self.scheme["Formula1"]==gongshi,"Month":"Value1"]
            fa["指标"] = Indicator2
            self.gongshi_file = self.gongshi_file.append(fa)
            # 自调用
            Calculate.engine(self,scheme=self.scheme,Indicator=Indicator2)
                

        # 情况3：有公式，但公式中没有指标名称
        if gongshi != "禅" and len(Indicator_list) == 0:

            # 3.1 调用计算模块，自动把结果存入数据池
            Calculate.calculate_formula(self,scheme,Indicator)
    
            # 3.2判断是否满足输出要求
            keys = list(self.fruit_basket.keys())
            Indi = [self.Indicator]
            # 3.3T 满足输出需要，处理返回计算结果事宜
            if set(Indi) <= set(keys):
                # print("计算完成！")
                pass

            # 3.3F 不满足输出要求，更改方案内容后继续解析
            else:
                # 3.3F1 把水果篮中的键值都替换到方案中
                # for k,v in self.fruit_basket.items():
                #     scheme.loc[:,"Formula1":"Value15"] = scheme.loc[:,"Formula1":"Value15"].apply(lambda x: x.replace("(.*?)({})(.*)".format(k),"\\1烟雨{}\\3".format(v),regex=True))
                #     scheme.loc[:,"Formula1":"Value15"] = scheme.loc[:,"Formula1":"Value15"].apply(lambda x: x.replace("烟雨","",regex=True))
                fb2 = copy.deepcopy(self.fruit_basket)
                [fb2.pop(k) for k,v in self.yjtq.items()]
                # 1.3F1 把水果篮中的键值都替换到方案中
                for k,v in fb2.items():
                    self.yjtq[k] = v
                    # print("要替换的东西",k,v)
                    # print("替换前",scheme.loc[:,"Month":"Value1"],"\n")
                    scheme.loc[:,"Formula1":"Value15"] = scheme.loc[:,"Formula1":"Value15"].apply(lambda x: x.replace(r"(.*[\+\-\*\/\=\!\(\)\<\>\'\"\s]+)({})([\'\"\+\-\*\/\=\!\(\)\>\<\s]+.*)".format(k),r"\1烟雨{}\3".format(v),regex=True))
                    scheme.loc[:,"Formula1":"Value15"] = scheme.loc[:,"Formula1":"Value15"].apply(lambda x: x.replace(r"^({})([\+\-\*\/\=\!\(\)\>\<\'\"\s]+.*)$".format(k),r"{}\2".format(v),regex=True))
                    scheme.loc[:,"Formula1":"Value15"] = scheme.loc[:,"Formula1":"Value15"].apply(lambda x: x.replace(r"^(.*[\+\-\*\/\=\!\(\)\>\<\'\"\s]+)({})$".format(k),r"\1烟雨{}".format(v),regex=True))
                    scheme.loc[:,"Formula1":"Value15"] = scheme.loc[:,"Formula1":"Value15"].apply(lambda x: x.replace(r"^{}$".format(k),v,regex=True))
                    scheme.loc[:,"Formula1":"Value15"] = scheme.loc[:,"Formula1":"Value15"].apply(lambda x: x.replace("烟雨","",regex=True))
                    # print("替换后",scheme.loc[:,"Month":"Value1"],"\n")

                # 3.3F2 从头开始处理上一级的指标与公式
                # 找出上级指标
                sjzb = self.gongshi_file.loc[self.gongshi_file["指标"]==Indicator,"Indicator"]
                sjzb = list(sjzb)[0]
                # 子调用
                Calculate.engine(self,scheme=self.scheme,Indicator=sjzb)


    def main(self):
        # 获取岗位级计算方案
        Calculate.get_scheme(self)
        # 开始计算
        Calculate.engine(self,scheme=self.scheme,Indicator=self.Indicator)
        return self.fruit_basket


if __name__ == "__main__":
    # p = Calculate(Month="2020-06-01",Project="sc0003",Business="sc000301",Post="客服专员",Indicator="产量奖",ICN="510623199202213812")
    p = Calculate(Month="2020-07-01",Project="sc0003",Business="sc000301",Post="客服专员",Indicator="绩效工资",ICN="510824199610012566")
    begin = datetime.now()
    print("计算结果：",p.main())
    end = datetime.now()
    print("计算耗时: ",end - begin)
