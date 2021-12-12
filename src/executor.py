"""Main Process Executor"""

from dataclasses import dataclass
from enum import IntEnum
from io import FileIO
from pathlib import Path
from typing import List, Tuple, Final

import pandas as pd
import numpy as np


OUT_DIR: Final[str] = "./cache"

FN_RESULT: Final[str] = "学生情報・DONGURIアカウント情報紐付け結果一覧.xlsx"
FP_RESULT: Final[str] = f"{OUT_DIR}/{FN_RESULT}"
SHN_RESULT_BUYER: Final[str] = "購入者"
SHN_RESULT_NO_BUYER: Final[str] = "非購入者"

FN_FAILED_STUDENTS: Final[str] = "DONGURIアカウント情報紐付けに失敗した学生一覧.xlsx"
FP_FAILED_STUDENTS: Final[str] = f"{OUT_DIR}/{FN_FAILED_STUDENTS}"
SHN_FAILED_STUDENTS_JYG: Final[str] = "生徒一覧"
SHN_FAILED_STUDENTS_CMS: Final[str] = "購入情報-マッチング候補"

FN_REST_DONGURI_ACC: Final[str] = "DONGURI残りのアカウント一覧.xlsx"
FP_REST_DONGURI_ACC: Final[str] = f"{OUT_DIR}/{FN_REST_DONGURI_ACC}"
SHN_REST_DONGURI_ACC_5DIC: Final[str] = "5辞書アカウント"
SHN_REST_DONGURI_ACC_2DIC: Final[str] = "2辞書アカウント"


@dataclass
class BuyingDicType:
    DIC_5: str='5辞書'
    DIC_2: str='2辞書'
    DIC_NONE: str='購入しない'
    NULL: str='（手動で作成）'

buying_dic_type = BuyingDicType()


class CmsData:
    def __init__(self, csv_file):
        self.load_prep(csv_file)

    def load_prep(self, csv_file) -> None:
        self.data = pd.read_csv(csv_file, encoding="shift-jis")
        # prep : XXX(kana) --> XXX
        _temp = self.data['生徒名'].copy()
        _temp = _temp.str.split('(', expand=True)[0] # exclude (kana)
        _temp = _temp.str.strip() # remove white space
        _temp = _temp.str.replace("　", "") # remove ZENKAKU space
        self.data['生徒名'] = _temp.copy()

    def get_student_id(self) -> pd.Series:
        return self.data[self.join_target_col()].copy()

    def join_target_col(self) -> str:
        return '学籍番号'

    def get_names(self) -> pd.Series:
        return self.data['生徒名'].copy()

    def calc_dict_buy_type(self) -> None:
        _dict_buy_5_flg = (self.data['同時購入品1NO'] > 0)
        _dict_buy_2_flg = (self.data['同時購入品2NO'] > 0)
        _dict_buy_n_flg = ~(_dict_buy_5_flg | _dict_buy_2_flg)

        _dict_buy_type: List[str] = []
        buying_dic_type = BuyingDicType()
        for d5, d2, dno in zip(_dict_buy_5_flg, _dict_buy_2_flg, _dict_buy_n_flg):
            _cur_type = buying_dic_type.DIC_NONE
            if d5 == True:
                _cur_type = buying_dic_type.DIC_5
            elif d2 == True:
                _cur_type = buying_dic_type.DIC_2
            _dict_buy_type.append(_cur_type)

        self.data['副教材タイプ'] = _dict_buy_type


class DonguriAccount:
    def __init__(self, exl_file):
        self.load_prep(exl_file)

    def load_prep(self, exl_file) -> None:
        self.data = pd.read_excel(exl_file)

    def get_head(self, num: int) -> pd.DataFrame:
        self.used_acc_num = min(num, self.data.shape[0])
        return self.data.head(num).copy()

    def get_rest_acc_num(self) -> int:
        return (self.data.shape[0] - self.used_acc_num)

    def get_rest_of(self) -> pd.DataFrame:
        return self.data.tail(self.get_rest_acc_num())


class JiyuStudents:
    def __init__(self, csv_file):
        self.load_prep(csv_file)

    def load_prep(self, csv_file) -> None:
        self.data = pd.read_csv(csv_file, encoding='utf-8')
        _temp = self.data[self.get_name_col_name()].copy()
        _temp = _temp.str.strip() # remove white space
        _temp = _temp.str.replace("　", "") # remove ZENKAKU space
        self.data[self.get_name_col_name()] = _temp.copy()
        # CMSとマッチングできるように型をstrにする
        self.data[self.join_target_col()] = self.data[self.join_target_col()].astype(str)

    def get_student_test_id(self) -> pd.Series:
        return self.data[self.join_target_col()].copy()

    def get_names(self) -> pd.Series:
        return self.data[self.get_name_col_name()].copy()

    def get_name_col_name(self) -> str:
        return '氏\u3000名'

    def join_target_col(self) -> str:
        return 'テスト番号'


class ShiraishiExecutor:
    def __init__(self, cms_file, dng5_file, dng2_file, jyg_file) -> None:
        self._cms_data = CmsData(cms_file)
        self._dongri_data_5dic = DonguriAccount(dng5_file)
        self._dongri_data_2dic = DonguriAccount(dng2_file)
        self._jiyu_students = JiyuStudents(jyg_file)
        Path(OUT_DIR).mkdir(parents=True, exist_ok=True)

    def main_func(self):
        self.__extract_newbee_from_cmsdata()
        self.__calc_dic_buying_type()
        self.__merge_cms_and_jyg()
        self.__concat_donguri_acc_and_cmsjyg()
        self.__export()

    def __extract_newbee_from_cmsdata(self):
        """## 1. Extract Newbee from CmsData
        """
        _newbee_flgs = self._cms_data.data['教科書タイトル'].str.contains('1年')
        self._cms_data.data = self._cms_data.data[_newbee_flgs]

    def __calc_dic_buying_type(self):
        """## 2. Calc Dictionary Buying Type (2dic or 5dic or None)

            - No1 - 5辞書
            - No2 - 2辞書
        """
        self._cms_data.calc_dict_buy_type()

    def __merge_cms_and_jyg(self):
        """## MERGE - CMS and Juyugaoka Students
        """
        self._merged_cms_jiyu = pd.merge(self._jiyu_students.data,
                                    self._cms_data.data,
                                    how='left',
                                    left_on=self._jiyu_students.join_target_col(),
                                    right_on=self._cms_data.join_target_col())

        # extract cols
        __target_cols = ['テスト番号', '合格学科', 'クラス２', '出席番号', '氏　名', 'id', '学籍番号', '生徒名', '教科書タイトル', '副教材タイプ']
        self._merged_cms_jiyu = self._merged_cms_jiyu[__target_cols]

        # '副教材タイプ' fill na -> BuyingDicType.NULL
        self._merged_cms_jiyu['副教材タイプ'].fillna(buying_dic_type.NULL, inplace=True)


    def __concat_donguri_acc_and_cmsjyg(self):
        """## CONCAT/MERGE - DONGURI ACC and CMS-JYG

            ### Note
            - アカウント数と生徒数を比べると、「5辞書、2辞書いずれも購入しない生徒」もいるようだ

            ### Processes
            1. Checking
            2. RESULTS1 - Attach account info to students rows
            3. RESULTS2 - No Buying data
            4. RESULTS3 - To Manual Operate Data
                1. CMSデータとマッチングできなかった jyg データ
                2. jyg データとマッチングしていない CMSデータ（新1年のみ）
        """
        # -------------------------------------------------
        # 1. Checking
        # - `[memo]` 名前マッチングからテスト番号-学籍番号マッチングに変更することで、失敗数が84件から18件に減った。
        # Split CMS-JYG into DIC_5/DIC_2/DIC_NONE
        __merged_cms_jiyu_d5 = self._merged_cms_jiyu[self._merged_cms_jiyu['副教材タイプ'] == buying_dic_type.DIC_5].copy()
        __merged_cms_jiyu_d2 = self._merged_cms_jiyu[self._merged_cms_jiyu['副教材タイプ'] == buying_dic_type.DIC_2].copy()
        __merged_cms_jiyu_dN = self._merged_cms_jiyu[self._merged_cms_jiyu['副教材タイプ'] == buying_dic_type.DIC_NONE].copy()
        __merged_cms_jiyu_NaN = self._merged_cms_jiyu[self._merged_cms_jiyu['副教材タイプ'] == buying_dic_type.NULL].copy()

        _total_row = self._merged_cms_jiyu.shape[0]

        print(f'== 同時購入品として5辞書、2辞書いずれかを選んだ生徒')
        print(f'5辞書 購入者数 = {__merged_cms_jiyu_d5.shape[0]} / {_total_row} (準備済みアカウント数: {self._dongri_data_5dic.data.shape[0]})')
        print(f'2辞書 購入者数 = {__merged_cms_jiyu_d2.shape[0]} / {_total_row} (準備済みアカウント数: {self._dongri_data_2dic.data.shape[0]})')
        print()

        print(f'== 同時購入品として5辞書、2辞書いずれかも選んでない生徒')
        print(f'非購入者数 = {__merged_cms_jiyu_dN.shape[0]} / {_total_row}')
        print()

        print(f'== 文字化けなどを理由にCMS側に対応するデータが見つけられなかった生徒（手動作業）')
        print(f'手動オペレーション対象者数 = {__merged_cms_jiyu_NaN.shape[0]} / {_total_row}')
        print()

        # -------------------------------------------------
        # 2. RESULTS1 - Attach account info to students rows
        # dic 5
        _acc_rows_d5 = self._dongri_data_5dic.get_head(__merged_cms_jiyu_d5.shape[0])
        # -- concat cols は index が一致するものをつなぐので整えておく
        __merged_cms_jiyu_d5.reset_index(drop=True, inplace=True)
        _acc_rows_d5.reset_index(drop=True, inplace=True)
        _cms_jyg_acc_d5 = pd.concat([__merged_cms_jiyu_d5, _acc_rows_d5], axis=1) # axis=columns,1


        # dic 2
        _acc_rows_d2 = self._dongri_data_2dic.get_head(__merged_cms_jiyu_d2.shape[0])
        # -- concat cols は index が一致するものをつなぐので整えておく
        __merged_cms_jiyu_d2.reset_index(drop=True, inplace=True)
        _acc_rows_d2.reset_index(drop=True, inplace=True)
        _cms_jyg_acc_d2 = pd.concat([__merged_cms_jiyu_d2, _acc_rows_d2], axis=1) # axis=columns,1

        # dic None
        # -- 不要

        # Concat vertically
        self.cms_jyg_acc = pd.concat([_cms_jyg_acc_d5, _cms_jyg_acc_d2], axis=0) # axis=rows:0
        self.cms_jyg_acc.fillna(value="アカウント不足", inplace=True)

        # -------------------------------------------------
        # 3. RESULTS2 - No Buying data
        self.cms_jyg_no_buyer = __merged_cms_jiyu_dN.copy()
        self.cms_jyg_no_buyer.reset_index(drop=True, inplace=True)

        # -------------------------------------------------
        # 4. RESULTS3 - To Manual Operate Data
        # CMSデータとマッチングできなかった jyg データ
        self.jyg_manual_operate = __merged_cms_jiyu_NaN[['テスト番号', '合格学科', 'クラス２', '出席番号', '氏　名']].copy()
        self.jyg_manual_operate.reset_index(drop=True, inplace=True)

        # jyg データとマッチングしていない CMSデータ（新1年のみ、前処理で抽出済み）
        # -- マッチング成功した '学籍番号' 一覧取得
        _successfully_matched_ids = []
        _successfully_matched_ids.extend(self.cms_jyg_acc['学籍番号'].values)
        _successfully_matched_ids.extend(self.cms_jyg_no_buyer['学籍番号'].values)
        # -- これを含まないCMSデータだけ取得
        self._cms_newbee_unmatched = self._cms_data.data[~self._cms_data.data['学籍番号'].isin(_successfully_matched_ids)].copy()
        __target_cols = ['id', '学籍番号', '生徒名', '教科書タイトル', '副教材タイプ']
        self._cms_newbee_unmatched = self._cms_newbee_unmatched[__target_cols]
        self._cms_newbee_unmatched.reset_index(drop=True, inplace=True)

    def __export(self):
        """## EXPORT

            ### Note
                - 下記を分けて出力する
                - CMSに対応データが見つかったデータ（自動算出 **成功**）
                - 学籍番号入力ミスなどにより、CMSに対応データが見つからなかったデータ（自動算出 **失敗**）
                    - ★手動オペレーションに利用
                - 残りのアカウント情報 - 5辞書
                - 残りのアカウント情報 - 2辞書

            ### Fmt
                - Excel File: `学生情報・DONGURIアカウント情報紐付け結果一覧.xlsx`
                    - Sheet: `購入者` - RESULTS1
                    - Sheet: `非購入者` - RESULTS2
                - Excel File: `DONGURIアカウント情報紐付けに失敗した学生一覧.xlsx`
                    - Sheet: `生徒一覧` - RESULTS3
                    - Sheet: `購入情報-マッチング候補` - RESULTS3
                - Excel File: `DONGURI残りのアカウント一覧.xlsx`
                    - Sheet: `5辞書アカウント` - RESULTS4
                    - Sheet: `2辞書アカウント` - RESULTS4

            https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_excel.html?highlight=to_excel#pandas.DataFrame.to_excel
        """
        with pd.ExcelWriter(FP_RESULT) as writer:
            self.cms_jyg_acc.to_excel(writer, sheet_name=SHN_RESULT_BUYER, index=False)
            self.cms_jyg_no_buyer.to_excel(writer, sheet_name=SHN_RESULT_NO_BUYER, index=False)

        with pd.ExcelWriter(FP_FAILED_STUDENTS) as writer:
            self.jyg_manual_operate.to_excel(writer, sheet_name=SHN_FAILED_STUDENTS_JYG, index=False)
            self._cms_newbee_unmatched.to_excel(writer, sheet_name=SHN_FAILED_STUDENTS_CMS, index=False)

        with pd.ExcelWriter(FP_REST_DONGURI_ACC) as writer:
            self._dongri_data_5dic.get_rest_of().to_excel(writer, sheet_name=SHN_REST_DONGURI_ACC_5DIC, index=False)
            self._dongri_data_2dic.get_rest_of().to_excel(writer, sheet_name=SHN_REST_DONGURI_ACC_2DIC, index=False)
