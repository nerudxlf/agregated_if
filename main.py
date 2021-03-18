import re

import pandas as pd


def filter_scopus(data: object) -> object:
    data = data.filter(["Authors", "Title", "Source Title"])
    source_title_list = data["Source Title"].to_list()
    title_list = data["Title"].to_list()
    key_j_list = []
    key_title_list = []
    for i in source_title_list:
        i = i.split(" (")[0]
        key_j_list.append((re.sub("[^A-Za-z0-9]", "", i)).upper())
    for i in title_list:
        key_title_list.append((re.sub("[^A-Za-z0-9]", "", i)).upper())
    data["KEY_J"] = key_j_list
    data["KEY_T"] = key_title_list
    return data


def filter_wos(data: object) -> object:
    data = data.filter(["Authors", "Article Title", "Source Title"])
    data.rename(columns={"Article Title": "Title"}, inplace=True)
    source_title_list = data["Source Title"].to_list()
    title_list = data["Title"].to_list()
    key_j_list = []
    key_title_list = []
    for i in source_title_list:
        i = i.split(" (")[0]
        key_j_list.append((re.sub("[^A-Za-z0-9]", "", i)).upper())
    for i in title_list:
        key_title_list.append((re.sub("[^A-Za-z0-9]", "", i)).upper())
    data["KEY_J"] = key_j_list
    data["KEY_T"] = key_title_list
    return data


def scimago_filter(data: object) -> object:
    data = data.filter(["Title", "SJR"])
    data.rename(columns={"Title": "Source Title"}, inplace=True)
    source_title_list = data["Source Title"].to_list()
    key_list = []
    for i in source_title_list:
        i = i.split(" (")[0]
        key_list.append((re.sub("[^A-Za-z0-9]", "", i)).upper())
    data["KEY_J"] = key_list
    return data


def journal_list_filter(data: object) -> object:
    data = data.filter(["Full Journal Title", "Journal Impact Factor"])
    data.rename(columns={"Full Journal Title": "Source Title"}, inplace=True)
    source_title_list = data["Source Title"].to_list()
    key_list = []
    for i in source_title_list:
        i = i.split(" (")[0]
        key_list.append((re.sub("[^A-Za-z0-9]", "", i)).upper())
    data["KEY_J"] = key_list
    return data


def main():
    wos_data_df = filter_wos(pd.read_excel("wos2020.xls"))
    scopus_data_df = filter_scopus(pd.read_excel("scopus2020.xlsx"))
    scimago_data_df = scimago_filter(pd.read_csv("scimagojr_2019.csv", sep=';', ))
    impact_factor_df = journal_list_filter(pd.read_excel("journal_list_jcr_2019.xlsx"))

    con_data = pd.concat([scopus_data_df, wos_data_df])
    con_data.drop_duplicates(subset=["KEY_T"], inplace=True)

    result_data = pd.merge(left=con_data, right=impact_factor_df, left_on="KEY_J", right_on="KEY_J", how="left")
    result_data = pd.merge(left=result_data, right=scimago_data_df, left_on="KEY_J", right_on="KEY_J", how="left")
    result_data = result_data.filter(
        ["Authors", "Title", "Source Title_x", "Source Title_y", "SJR", "Journal Impact Factor"])
    result_data.to_excel("result_new.xlsx", index=False)
