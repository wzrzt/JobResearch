

import os
import time
from tqdm import tqdm
import requests
import pandas as pd
from lxml import etree
import json
import random




url_init = """https://www.payscale.com/research/HK/Job=Customer_Success_Manager/Salary"""
url_base = """https://www.payscale.com/research/{region_code}/Job={job_title}/Salary"""

headers = {
    "User-Agent": """Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36""",
}
xpath_data = """//*[@id="__NEXT_DATA__"]/text()"""

# res_temp = requests.get(url_init, headers=headers)
# page_html = etree.HTML(res_temp.text)
# data_str = page_html.xpath(xpath_data)
# data_json = json.loads(data_str[0])
# with open('test.json', 'w') as f1:
#     json.dump(data_json, f1)


region_dict = {
    'HK': "Hong Kong",
    'SG': "Singapore",
    "HU": "Hungary",
    "PL": "Poland",
    "PH": "Philippines",
    "ID": "Indonesia",
    "TH": "Thailand",
    "MY": "Malaysia"
}


# proxies = {
#    'http': 'socks5://192.168.3.252:7893',
#    'https': 'socks5://192.168.3.252:7893',
# }


def read_history(history_file):
    if os.path.isfile(history_file):
        df_history = pd.read_csv(history_file, dtype=str, header=None,
                                names=['Q10','Q25','Q50','Q75','Q90','profileCount','type','title','title_sim','job_id']
                                )
        return df_history
    else:
        return pd.DataFrame()
    


def get_similar_titles(keyword):
    region_str = region_dict[region_code]
    req_param = {"Research_Center_All": keyword,
                    "country": region_str}
    url_similar_job = """https://www.payscale.com/complete.aspx"""

    res = requests.get(url_similar_job, headers=headers, params=req_param #, proxies=proxies
                       )

    res_json = res.json()

    return res_json



def result_parser(json_in):
    part_compensation = json_in['props']['pageProps']['pageData']['compensation']
    out_list = []
    for k, v in part_compensation.items():
        tmp_out = v.copy()
        tmp_out['type'] = k
        out_list.append(tmp_out)

    out_df = pd.DataFrame(out_list)

    part_payby_exp = json_in['props']['pageProps']['pageData']['byDimension']['Job by Experience']['rows']

    df_payby_exp = pd.DataFrame(part_payby_exp)

    out_dict = {
        'compensation': out_df,
        'payby_exp': df_payby_exp
    }
    
    return out_dict

# 读取JobTitle清单
df_titles = pd.read_excel('Input/JobTitles.xlsx', sheet_name='title-full', dtype=str)


# for region_code in ["PH", "ID", "TH", "MY"]:
# for region_code in ["ID", "TH", "MY"]:
for region_code in ["MY"]:
    # crawl_batch = 'PL-FULL-20230330'
    # region_code = 'PL'
    crawl_batch = f"{region_code}-FULL-20231210"
    title_cnt = df_titles.shape[0]

    compensation_path = os.path.join('MidOutput', f'compensation-{crawl_batch}.csv')
    payby_exp_path = os.path.join('MidOutput', f'payby_exp-{crawl_batch}.csv')

    compensation_list = []
    payby_exp_list = []

    compensation_history = read_history(compensation_path)
    if compensation_history.empty:
        max_job_id = 0
    else:
        max_job_id = max(compensation_history['job_id'].astype(int))

    pbar = tqdm(df_titles.iterrows(), total=title_cnt)
    for row_id, row in pbar:

        if row_id < max_job_id-1:
            print(f"Job_id <= {max_job_id} 已抓取，跳过")
            continue
        raw_title = row['title']
        tmp_title = row['title'].replace(' ', '_')
        job_id = row_id + 1

        pbar.set_description(f"{tmp_title}")
        
        tmp_url = url_base.format(region_code=region_code, job_title = tmp_title)
        print(tmp_url)
        tmp_res = requests.get(tmp_url, headers=headers # , proxies=proxies
                            )
        tmp_html = etree.HTML(tmp_res.text)
        tmp_res_str = tmp_html.xpath(xpath_data)
        tmp_res_str_len = len(tmp_res_str)

        if tmp_res_str_len > 0:
            tmp_json = json.loads(tmp_res_str[0])
            tmp_data = result_parser(tmp_json)

            tmp_compensation = tmp_data['compensation']
            tmp_payby_exp = tmp_data['payby_exp']

            tmp_compensation['title'] = raw_title
            tmp_payby_exp['title'] = raw_title
            tmp_compensation['title_sim'] = None
            tmp_payby_exp['title_sim'] = None
            tmp_compensation['job_id'] = job_id
            tmp_payby_exp['job_id'] = job_id
            
        else:

            sim_titles = get_similar_titles(raw_title)

            if len(sim_titles) > 0:
            
                sub_pbar = tqdm(sim_titles)
                sub_compensation_list = []
                sub_payby_exp_list = []
                for sim_title in sub_pbar:
                    sub_pbar.set_description(sim_title)
                    
                    tmp_url = url_base.format(region_code=region_code, job_title = sim_title.replace(' ', '_'))
                    print(tmp_url)
                    tmp_res = requests.get(tmp_url, headers=headers # , proxies=proxies
                                        )
                    tmp_html = etree.HTML(tmp_res.text)
                    tmp_res_str = tmp_html.xpath(xpath_data)
                    tmp_res_str_len = len(tmp_res_str)

                    if tmp_res_str_len > 0:
                        tmp_json = json.loads(tmp_res_str[0])
                        tmp_data = result_parser(tmp_json)

                        tmp_compensation = tmp_data['compensation']
                        tmp_payby_exp = tmp_data['payby_exp']

                        tmp_compensation['title'] = raw_title
                        tmp_payby_exp['title'] = raw_title
                        tmp_compensation['title_sim'] = sim_title
                        tmp_payby_exp['title_sim'] = sim_title
                        tmp_compensation['job_id'] = job_id
                        tmp_payby_exp['job_id'] = job_id
                        
                        sub_compensation_list.append(tmp_compensation)
                        sub_payby_exp_list.append(tmp_payby_exp)

                    wait_random = random.randint(1, 10000) / 1000
                    time.sleep(5 + wait_random)
                
                if sub_compensation_list:
                    tmp_compensation = pd.concat(sub_compensation_list)
                else:
                    tmp_compensation = pd.DataFrame()
                if sub_payby_exp_list:
                    tmp_payby_exp = pd.concat(sub_payby_exp_list)
                else:
                    tmp_payby_exp = pd.DataFrame()
            else: 
                continue

        compensation_list.append(tmp_compensation)
        payby_exp_list.append(tmp_payby_exp)

        with open(compensation_path, 'a+') as f1:
            tmp_compensation.to_csv(f1, index=False, header=False)

        with open(payby_exp_path, 'a+') as f1:
            tmp_payby_exp.to_csv(f1, index=False, header=False)

        wait_random = random.randint(1, 10000) / 1000
        time.sleep(10 + wait_random)
        print(len(compensation_list))

    df_compensation = pd.concat(compensation_list)
    df_payby_exp = pd.concat(payby_exp_list)

    rename_dict = {"10": "Q10", "25": "Q25", "50": "Q50", "75": "Q75", "90": "Q90"}
    df_compensation = df_compensation.rename(rename_dict, axis=1)

    with pd.ExcelWriter(f'/Users/wzr/Downloads/jobCrawled-{crawl_batch}.xlsx') as writer:
        df_compensation.to_excel(writer, sheet_name='compensation', index=False)
        df_payby_exp.to_excel(writer, sheet_name='payby_exp', index=False)

    print(1)

    df1 = pd.read_csv(f'compensation-{crawl_batch}.csv', dtype=str, header=None,
                        names=['Q10','Q25','Q50','Q75','Q90','profileCount','type','title','title_sim','job_id'])

    df3 = pd.read_csv(f'payby_exp-{crawl_batch}.csv', dtype=str, header=None, 
                    names = ['name','displayName','url','profileCount','range',
                            'isEstimated','title','title_sim','job_id']
                            )
    with pd.ExcelWriter(f'/Users/wzr/Downloads/jobCrawled-merged-{crawl_batch}.xlsx') as writer:
        df1.to_excel(writer, sheet_name='compensation', index=False)
        df3.to_excel(writer, sheet_name='payby_exp', index=False)

