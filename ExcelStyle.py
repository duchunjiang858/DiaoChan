import pandas as pd
import re

pd.set_option('display.unicode.ambiguous_as_wide', True)
pd.set_option('display.unicode.east_asian_width', True)
pd.set_option('display.float_format',lambda x : '%.2f' % x)
pd.set_option('display.max_colwidth', 20)

def beautify():
    sheet = pd.read_excel(r"D:\anaconda3\Lib\site-packages\DiaoChan\attachment\指标计算逻辑.xlsx",sheet_name="绩效方案")
    sheet = sheet.dropna(axis = 0,how = 'all')
    sheet = sheet.where(sheet.notnull(),"禅")  # 把np.nan替换掉
    # sheet = sheet.apply(lambda x: x.replace(r"\s","",regex=True))
    sheet = sheet.apply(lambda x: x.replace(r"（","(",regex=True))
    sheet = sheet.apply(lambda x: x.replace(r"）",")",regex=True))
    sheet = sheet.apply(lambda x: x.replace(r"！","!",regex=True))
    # sheet = sheet.apply(lambda x: x.replace(r"([^=!<>]+)(=)(\d+)$",r"\1\2\2\3",regex=True))
    # sheet = sheet.apply(lambda x: x.replace(r"^([^=]*)([!<>]=)((?:[^=!<>\d]+[\d]*)|(?:[\d]*[^=!<>\d]+))$",r"{}\1{}\2{}\3{}".format("'","'","'","'"),regex=True))
    # sheet = sheet.apply(lambda x: x.replace(r"([^=!<>]+)(=)((?:[^=!<>\d]+[\d]*)|(?:[\d]*[^=!<>\d]+))$",r"{}\1{}\2\2{}\3{}".format("'","'","'","'"),regex=True))
    # sheet = sheet.apply(lambda x: x.replace(r"(.*?)(and|or)(.*>)",r'\1 \2 \3',regex=True))
    return sheet


if __name__ =='__main__':
    jxfa_sheet = beautify()
    print(jxfa_sheet)
    # jxfa_sheet.to_excel(r"C:\Users\user\Desktop\新建.xlsx")
    
