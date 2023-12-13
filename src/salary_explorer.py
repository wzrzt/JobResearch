

import requests
from lxml import etree
import pandas as pd


output_filepath = 'Output/salary_explorer/PH_FI_by_job.xlsx'
url = "https://www.salaryexplorer.com/average-salary-wage-comparison-philippines-accounting-and-finance-c215f4"

res = requests.get(url)

html = etree.HTML(res.text)

xpath_table = '//*[@id="contentDiv"]/div[2]/div[1]/table'

node_table = html.xpath(xpath_table)

node_table_str = etree.tostring(node_table[0])

df1 = pd.read_html(node_table_str)[0]
df1.columns = ['Job Title', 'Average Salary']
df1.to_excel(output_filepath, index=False)

print('finished')

