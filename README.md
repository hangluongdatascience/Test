#1. Load library
from pathlib import Path

from openpyxl import load_workbook
from collections import defaultdict
from tqdm import tqdm
from openpyxl.utils.dataframe import dataframe_to_rows
import numpy as np
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE
import pandas as pd
from datetime import datetime, timedelta
import sys

import core_pro
sys.path.extend([str(Path.home() / "PycharmProjects/automation")])
from core_pro import Gdrive, Sheet, DataPipeLine
from core_pro.Gdrive import Drive
from core_pro.ultilities import update_df, make_dir, remove_old_file
from core_pro.seatalk_bot import seatalk_notification
from shop_report_v2.new_input import *
import os
import pygsheets
# authenticate:
gc = pygsheets.authorize(service_file= 'C:\\Users\\hang.luongthi\\PycharmProjects\\automation\\token\\shop_report_service_account.json')
from GoogleApiSupport import drive
import gspread
# authenticate:
gd = gspread.service_account(filename= 'C:\\Users\\hang.luongthi\\PycharmProjects\\automation\\token\\shop_report_service_account.json')

# # 2. notify starting
# group_id = 'https://openapi.seatalk.io/webhook/group/DBXYes6GRuao314skk1UvQ'
# text = "start testing new template"
# seatalk_notification(group_id=group_id, text=text)
# print('Done sending starting noti')

#3. If data loaded to csv today, we won't load it again for resource saving.
#3.1 get today value
today_date = datetime.today().strftime('%Y-%m-%d')

#3.2 directionary for save csv file
cache_dir=Path("C:\\Users\\hang.luongthi\\OneDrive - Seagroup\\paid_ads\\get_data_csv")

# #3.3 delete all old file
# all_files = list(cache_dir.glob('**/*'))
#
# for temp_file in all_files:
#     _date = temp_file.name[:-4].split('_')[1:]
#     if _date!=today_date:
#         os.remove(temp_file)

# 3.4 get data: if exists file csv of today before, use it now, else get file through jdbc file
# get raw data
df, sql, raw = {}, {}, {}
for i in ['org', 'all_ads', 'ads', 'keyword', 'item', 'all_ads_month', 'org_month', 'suggestion']:
    sql[i] = Path.home() / f'PycharmProjects/automation/shop_report_v2/sql_v2/{i}.sql'
    data_csv = cache_dir/f'{i}_{today_date}.csv'
    if os.path.exists(str(data_csv)):
        raw[i] = pd.read_csv(data_csv,encoding='utf-8-sig')
    else:
        raw[i] = DataPipeLine(sql[i], sql_file=True).run_presto_to_df()
        raw[i].to_csv(data_csv,index = False)

for frame in dict_.keys():
    raw[f'{frame}']['shop_id'] = raw[f'{frame}']['shop_id'].astype(str)
    raw[dict_.get(f'{frame}')[0]]['shop_id'] = raw[dict_.get(f'{frame}')[0]]['shop_id'].astype(str)
    raw[f'{frame}'] = process(raw[f'{frame}'], raw[dict_.get(f'{frame}')[0]], 'shop_id', dict_.get(f'{frame}')[1])
print('process: done getting data')

#4.get data and export fuction
## 4.1 get raw suggestion
sh = '1iyKTIzABVsRD20T8z7J_RZ5LjeMnKXUybvl6waUoC8M'
sheet = {}
for sheet_name in sheet_suggest_dict.keys():
    sheet[f'{sheet_name}'] = Sheet(sh).google_sheet_into_df(f'{sheet_name}', sheet_suggest_dict.get(sheet_name))
print('Done getting raw suggestion sheet')

# 4.2 date range
date_end = datetime.strftime(datetime.today() - timedelta(1), '%d-%m-%Y')
date_start = datetime.strftime(datetime.today() - timedelta(30), '%d-%m-%Y')
date = {'date_start': date_start,
         'date_end': date_end
         }
# 4.3 shop report
shop_run = Sheet('1TKsjt6IvxFxHDxchCWnn4DCsOFMTS4LRjZQWGQ4a8xc').google_sheet_into_df('overview', 'A1:F5000')
shop_lst = defaultdict(set)
for i in shop_run.values.tolist():
    name, shopid, s_type, cat = i[:4]
    subtype = i[5]
    shop_lst[s_type].add((shopid, name, cat, subtype))
#
# print('process: done getting shop')
# sql = "select * from vnbi_proj.paid_ad_manage_shop where run_this_week = 'yes' and shopid is not null"
# shop_run = DataPipeLine(sql).run_presto_to_df()
# shop_lst = defaultdict(set)
# for i in shop_run.values.tolist():
#     name, shopid, s_type, cat = i[:4]
#     subtype = i[5]
    # shop_lst[s_type].add((shopid, name, cat, subtype))
print('process: done getting shop')
#4.4 Export data through direction bellow:

export = Path('G:/My Drive/paid_ads/new_report')

for seller_type in ['Mall', 'CB', 'C2C']:
    for idx in tqdm(shop_lst.get(seller_type)):
        shop_id, shop_name, cat, subtype = idx
        shop_name = shop_name.strip()
        # folder path
        folder = export / f"{seller_type}/{cat}/{subtype}" if subtype in ('vip', 'Non-vip') else export / f"{seller_type}/{cat}"
        make_dir(folder)

        # data range
        if seller_type == 'CB':
            data_range = sheet['rule'].query(f'seller_type == "{seller_type}" & cat == "{cat}"').copy()
        else:
            data_range = sheet['rule'].query(f'seller_type == "{seller_type}"').copy()

        for i in ['date_start', 'date_end']:
            data_range.replace(f'<{i}>', date[f'{i}'], regex=True, inplace=True)
            # print(data_range['note'])
        try:
            note = str(data_range['note'].values[0])
        except IndexError:
            note = ''

        # data
        col = raw['all_ads'].columns[2:]
        tmp = raw['all_ads'].query(f'shop_id == "{shop_id}"').fillna(0)
        tmp_ad = raw['ads'].query(f'shop_id == "{shop_id}"').fillna(0)
        tmp_month = raw['all_ads_month'].query(f'shop_id == "{shop_id}"').fillna(0)
        tmp_suggestion = raw['suggestion'].query(f'shop_id == {shop_id}').fillna(0)
        # print(col, tmp, tmp_ad, tmp_suggestion)

        df['all_ads'] = tmp.pivot_table(columns=['week'], values=col)
        df['all_ads'] = df['all_ads'][df['all_ads'].columns[:-9:-1]].reset_index()
        all_ads_month = tmp_month.pivot_table(columns=['month'], values=col)
        all_ads_month = all_ads_month[all_ads_month.columns[:-7:-1]].reset_index()

        tmp_keyword = raw['keyword'].query(f'shop_id == {shop_id}').fillna(0)
        tmp_keyword = tmp_keyword.sort_values('ads_orders', ascending=False).head(10).reset_index(drop=True)
        keyword = tmp_keyword.drop(['shop_id'], axis=1)

        tmp_item = raw['item'].query(f'shop_id == {shop_id}').fillna(0)
        tmp_item = tmp_item.sort_values('ads_orders', ascending=False).head(10).reset_index(drop=True)
        item = tmp_item.drop(['shop_id'], axis=1)

        if not tmp_suggestion.empty:
            # fix illegal character
            keyword = keyword.applymap(lambda x: ILLEGAL_CHARACTERS_RE.sub(r'', x) if isinstance(x, str) else x)
            item = item.applymap(lambda x: ILLEGAL_CHARACTERS_RE.sub(r'', x) if isinstance(x, str) else x)

            for ad in tmp_ad.ads_type.unique():
                tmp = tmp_ad.query(f'ads_type == "{ad}"').copy()
                df[ad] = tmp.pivot_table(columns=['week'], values=col)
                df[ad] = df[ad][df[ad].columns[:-9:-1]].reset_index()

            # get template format base on their seller type and performance(bad/good)
            if not seller_type == 'Mall':
                df.pop('keyword:banner', None)

            if seller_type == 'Mall':
                for _ in df:
                    df[_].rename(columns={'index': 'Metric'}, inplace=True)
                    df[_]['Metric'] = df[_]['Metric'].map(translate_en)
                template = '1qu4GxSzlGwM-8fp-JKnqH-SiR42WUcLMWofQbLORlzw'
                if tmp_suggestion['roi'].values[0] > tmp_suggestion['roi_cat'].values[0]:
                    sheet_su = sheet['b2b_good'].copy()
                else:
                    sheet_su = sheet['b2b_bad'].copy()

            elif seller_type == 'CB':
                if cat == 'china':
                    for _ in df:
                        df[_].rename(columns={'index': 'Metric'}, inplace=True)
                        df[_]['Metric'] = df[_]['Metric'].map(translate_ch)
                    template = '1ZTUXHNqkN897eHwiiWty4sS9mG_ySSM3HQBygSSQH78'
                    if tmp_suggestion['roi'].values[0] > tmp_suggestion['roi_cat'].values[0]:
                        sheet_su = sheet['cb_good_cn'].copy()
                    else:
                        sheet_su = sheet['cb_bad_cn'].copy()
                else:
                    for _ in df:
                        df[_].rename(columns={'index': 'Metric'}, inplace=True)
                        df[_]['Metric'] = df[_]['Metric'].map(translate_en)
                    template = '1qu4GxSzlGwM-8fp-JKnqH-SiR42WUcLMWofQbLORlzw'
                    if tmp_suggestion['roi'].values[0] > tmp_suggestion['roi_cat'].values[0]:
                        sheet_su = sheet['cb_good_en'].copy()
                    else:
                        sheet_su = sheet['cb_bad_en'].copy()

            else:
                for _ in df:
                    df[_].rename(columns={'index': 'Metric'}, inplace=True)
                    df[_]['Metric'] = df[_]['Metric'].map(translate_vi)
                template = '1OTnHq2fo_RQSp0yHtbkQf0PZVLCH9B5AyEMS0PeczYU'
                if tmp_suggestion['roi'].values[0] > tmp_suggestion['roi_cat'].values[0]:
                    sheet_su = sheet['c2c_good'].copy()
                else:
                    sheet_su = sheet['c2c_bad'].copy()

            #create a worksheet by copy a template file
            sheet_template = gc.open_by_key(template)
            wb = gc.create("copy_sheet", folder_name=folder, template=sheet_template)
            #rename file_name
            file_name = shop_name
            rows = {a: dataframe_to_rows(df[a], index=False, header=True) for a in df}
            lst = list(rows)
            week_all_ads = df['all_ads']
            week_targeting = df['targeting:all']
            week_item_keyword = df['keyword:item']
            week_shop_keyword = df['keyword:shop']

            all_ads_month.rename(columns={'index': 'Metric'}, inplace=True)
            for i in replace_list:
                if isinstance(tmp_suggestion[f'{i}'].values[0], str):
                    sheet_su.replace(f'<{i}>', str(tmp_suggestion[f'{i}'].values[0]), regex=True, inplace=True)
                else:
                    sheet_su.replace(f'<{i}>', f"{round(tmp_suggestion[f'{i}'].values[0], 2):,}", regex=True,
                                     inplace=True)

            if seller_type == 'Mall':
                lst = [trans_kw_en[k] for k in lst]
                keyword.columns = keyword.columns.to_series().map(kw_en)
                item.columns = item.columns.to_series().map(kw_en)
                all_ads_month['Metric'] = all_ads_month['Metric'].map(translate_en)
                # get template and convert into data frame
                ws = wb.worksheet_by_title("Report")
                ws1 = wb.worksheet_by_title("Keyword")
                ws2 = wb.worksheet_by_title("raw_data")
                ws3 = wb.worksheet_by_title("Key Highlights")
                ws4 = wb.worksheet_by_title("Definition")

            elif seller_type == 'CB':
                if cat == 'china':
                    lst = [trans_kw_ch[k] for k in lst]
                    keyword.columns = keyword.columns.to_series().map(kw_ch)
                    item.columns = item.columns.to_series().map(kw_ch)
                    all_ads_month['Metric'] = all_ads_month['Metric'].map(translate_ch)

                    ws = wb.worksheet_by_title("Report (报告)")
                    ws1 = wb.worksheet_by_title("Product & Keywork（商品&关键字)")
                    ws2 = wb.worksheet_by_title("raw_data (原始数据)")
                    ws3 = wb.worksheet_by_title("Key Highlights (主要亮点)")
                    ws4 = wb.worksheet_by_title("Definition - 定义（双语)")

                else:
                    lst = [trans_kw_en[k] for k in lst]
                    keyword.columns = keyword.columns.to_series().map(kw_en)
                    item.columns = item.columns.to_series().map(kw_en)
                    all_ads_month['Metric'] = all_ads_month['Metric'].map(translate_en)

                    ws = wb.worksheet_by_title("Report")
                    ws1 = wb.worksheet_by_title("Keyword")
                    ws2 = wb.worksheet_by_title("raw_data")
                    ws3 = wb.worksheet_by_title("Key Highlights")
                    ws4 = wb.worksheet_by_title("Definition")


            else:
                lst = [trans_kw_vi[k] for k in lst]
                keyword.columns = keyword.columns.to_series().map(kw_vi)
                item.columns = item.columns.to_series().map(kw_vi)
                all_ads_month['Metric'] = all_ads_month['Metric'].map(translate_vi)

                ws = wb.worksheet_by_title("Báo Cáo")
                ws1 = wb.worksheet_by_title("Sản phẩm & Từ khóa")
                ws2 = wb.worksheet_by_title("raw_data")
                ws3 = wb.worksheet_by_title("Tóm tắt")
                ws4 = wb.worksheet_by_title("Chú thích")

            # data keywords - sheet keyword
            if keyword.empty == 'False':
                cell_keyword = pd.DataFrame(keyword).to_numpy()
                ws1.update_values('B5:O15', values=cell_keyword.tolist())

            #data of items- sheet keyword
            if item.empty == 'False':
                cell_item = pd.DataFrame(item).to_numpy()
                ws1.update_values('B18:O28', values=cell_item.tolist())

            #data weekly - sheet raw_data
            if week_all_ads.empty == 'False':
                cell_week_all_ads= pd.DataFrame(week_all_ads).to_numpy()
                ws2.update_values('A4:H18', values=cell_week_all_ads.tolist())

            if week_targeting.empty == 'False':
                cell_week_targeting= pd.DataFrame(week_targeting).to_numpy()
                ws2.update_values('A38:H52', values=cell_week_targeting.tolist())

            if week_item_keyword.empty == 'False':
                cell_week_item_keyword= pd.DataFrame(week_item_keyword).to_numpy()
                ws2.update_values('A55:H69', values=cell_week_item_keyword.tolist())

            if week_shop_keyword.empty == 'False':
                cell_week_shop_keyword= pd.DataFrame(week_shop_keyword).to_numpy()
                ws2.update_values('A21:H35', values=cell_week_shop_keyword.tolist())

            # #data monthly performance - sheet raw_data
            if all_ads_month.empty == 'False':
                cell_all_ads_month = pd.DataFrame(all_ads_month).to_numpy()
                ws2.update_values('A73:F87', values=cell_all_ads_month.tolist())

            #  data key highlights - sheet key highlights
            if sheet_su.empty == 'False':
                cell_sheet_su = pd.DataFrame(sheet_su).to_numpy()
                ws3.update_values('A3:B30', values=cell_sheet_su.tolist())
            # data update time range for "Definitions" sheet
            cell_a1 = ws4.cell('A1')
            cell_a1.value = note


            # choose a gspread method to open your spreadsheet and your worksheet:

            # update your worksheet name:
            wb.update_title(f"{shop_name}")

            print(folder / f"{shop_name}")

print('Done saving report to folder')

def get_link_file():

    page_token = None
    lst = defaultdict(list)

    # get file id
    for i in seller:
        while True:
            response = Drive().service.files().list(
                q=f'"{seller[i]}" in parents and trashed = false',
                spaces='drive',
                fields='nextPageToken, files(id, name)',
                pageToken=page_token).execute()
            for file in response.get('files', []):
                lst[i].append(file)
            page_token = response.get('nextPageToken', None)
            if page_token is None:
                break

    sql = "select * from vnbi_proj.paid_ad_manage_shop where run_this_week = 'yes' and shopid is not null"
    shop_id = DataPipeLine(sql).run_presto_to_df()

    link_dict = {}
    for f in seller:
        link_dict[f] = pd.DataFrame(lst[f])

    df = pd.concat([i for i in link_dict.values()])
    df['id'] = 'https://docs.google.com/spreadsheets/d/' + df['id']
    df['username'] = df['name'].str.split('_.xlsx', expand=True)[0]
    df = df.merge(shop_id[['shopid', 'username']], how='left', on='username')

    # export
    sh = '1-rbE9BPjoxS55ypQC1mSD1ii4rkVB10eTbiunUUiwio'
    update_df(df.fillna(''), 'new_link', sh)

get_link_file()

print('Done updating output to sheet')
#
# group_id = 'https://openapi.seatalk.io/webhook/group/DBXYes6GRuao314skk1UvQ'
# text = "Shop report update rồi nà: https://drive.google.com/drive/folders/1K8f9KTuM-OjXRFIGJzctksxhBhW9Jha_?usp=sharing"
# seatalk_notification(group_id=group_id, text=text)
#
# print('Done sending noti to group')



