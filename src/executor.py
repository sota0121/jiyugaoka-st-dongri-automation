"""Main Process Executor"""

from dataclasses import asdict, dataclass
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
SHN_REST_DONGURI_ACC_6DIC: Final[str] = "6辞書アカウント"
SHN_REST_DONGURI_ACC_3DIC: Final[str] = "3辞書アカウント"


PROD_NAME_DIC3: Final[str] = "【アプリ版辞書】DONGURI(3辞書)"
PROD_NAME_DIC6: Final[str] = "【アプリ版辞書】DONGURI(6辞書)"


@dataclass
class BuyingDicType:
    DIC_6: str='6辞書'
    DIC_5: str='5辞書'
    DIC_3: str='3辞書'
    DIC_2: str='2辞書'
    DIC_NONE: str='購入しない'
    NULL: str='（手動で作成）'

buying_dic_type = BuyingDicType()


@dataclass
class CmsDataCols:
    id: str = "ID"
    student_id: str = "学籍番号"
    student_name: str = "生徒名"
    student_name_kana: str = "生徒名（カナ）"
    email: str = "メールアドレス"
    registered: str = "registered"
    school_id: str = "学校ID"
    cur_school_year: str = "現在の学年"
    prod_name: str = "商品名"

DICTYPE_COL_NAME: Final[str] = "副教材タイプ"


class CmsData:

    def __init__(self, csv_file):
        self.cols = CmsDataCols()
        self.dictype = BuyingDicType()
        self.load_prep(csv_file)

    def load_prep(self, csv_file) -> None:
        """load and preparation"""

        # load csv file
        col_names = list(asdict(self.cols).values())
        self.data = pd.read_csv(
            csv_file,
            names=col_names,
            encoding="utf-8")

        # ----------------------------
        # preparation
        # ----------------------------
        # drop (student_id & name & prod_name) duplicated rows
        # causion! empty student_id exists
        self.data.drop_duplicates(
            subset=[self.cols.student_id, self.cols.student_name, self.cols.prod_name],
            inplace=True)

        # convert XXX(kana) --> XXX
        _temp = self.data[self.cols.student_name].copy()
        _temp = _temp.str.split('(', expand=True)[0] # exclude (kana)
        _temp = _temp.str.strip() # remove white space
        _temp = _temp.str.replace("　", "") # remove ZENKAKU space
        self.data[self.cols.student_name] = _temp.copy()

        # ----------------------------
        # Hotfix Data Transformation
        # -----
        # - 学籍番号が空の場合、異なる生徒間で学籍番号が重複してしまうための対応
        # - ただし、この対応でも網羅できないケースがある
        #   - 空文字ではない値で、生徒間で学籍番号が重複しているケース
        #   - 同一メールアドレスの生徒が複数学籍番号を空にしているケース
        # -----
        # - 恒久対応は、そもそも学籍番号が重複したデータを作らないようにシステムを改修すること
        # ----------------------------
        # 学籍番号が空の生徒のアドレス一覧(unique)を取得
        empty_id_students = self.data[self.data[self.cols.student_id].isna()].copy()
        empty_id_students = empty_id_students[self.cols.email].unique()

        # 仮のIDを生徒ごとに割り振る
        for i, email in enumerate(empty_id_students):
            virtual_id = 'empty_id_' + str(i)
            self.data.loc[self.data[self.cols.email] == email, self.cols.student_id] = virtual_id

        self.data.to_csv('output-loadprep.csv', index=False)


    def get_student_id(self) -> pd.Series:
        return self.data[self.join_target_col()].copy()

    def join_target_col(self) -> str:
        return self.cols.student_id

    def get_names(self) -> pd.Series:
        return self.data[self.cols.student_name].copy()

    def calc_dict_buy_type(self) -> None:
        """calculate dict buy type

            ### input

            ```
            | id | student_id | student_name | ... | prod_name |
            |----|------------|--------------|-----|-----------|
            | 1  | 123        | taro         | ... | 特進S1年   |
            | 2  | 123        | taro         | ... | X(3辞書)   |
            | 3  | 456        | jiro         | ... | 特進S1年   |
            | 4  | 789        | goro         | ... | 特進S1年   |
            | 5  | 789        | goro         | ... | X(6辞書)   |
            ```

            ### output

            ```
            | id | student_id | student_name | ... | 副教材タイプ |
            |----|------------|--------------|-----|------------|
            | 1  | 123        | taro         | ... | 3dic       |
            | 2  | 456        | jiro         | ... | 購入しない   |
            | 3  | 789        | goro         | ... | 6dic       |
            ```

        """
        # --------------------------------------------------------
        # Get Unique Student ID
        # --------------------------------------------------------
        uniq_student_id = self.get_student_id().unique()

        # --------------------------------------------------------
        # Judge dict buy type by student_id
        # --------------------------------------------------------
        dictype_by_uniq_students = []
        for student_id in uniq_student_id:
            student_orders = self.data[self.data[self.cols.student_id] == student_id].copy()

            # judge dict buy type
            if student_orders[self.cols.prod_name].isin([PROD_NAME_DIC6]).any():
                # 6dic
                dictype_by_uniq_students.append(self.dictype.DIC_6)
            elif student_orders[self.cols.prod_name].isin([PROD_NAME_DIC3]).any():
                # 3dic
                dictype_by_uniq_students.append(self.dictype.DIC_3)
            else:
                # 購入しない
                dictype_by_uniq_students.append(self.dictype.DIC_NONE)

        co_student_id_col = 'co_student_id'
        _df_id_dictype = pd.DataFrame(
            data={
                co_student_id_col: uniq_student_id,
                DICTYPE_COL_NAME: dictype_by_uniq_students}
            )


        # --------------------------------------------------------
        # Squash to 1 record per student_id
        # --------------------------------------------------------
        squashed_data = self.data.copy()
        squashed_data = squashed_data.drop_duplicates(subset=[self.cols.student_id])

        # --------------------------------------------------------
        # Set dict buy type
        # --------------------------------------------------------
        new_df = pd.merge(
            left=squashed_data,
            right=_df_id_dictype,
            left_on=self.cols.student_id,
            right_on=co_student_id_col,
            how='left')
        self.data = new_df.copy()

        # debug
        self.data.to_csv('output-calcdictype.csv', index=False)



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



@dataclass
class JiyuStuCols:
    exam_id: str = "テスト番号"
    course_name: str = "コース"
    class_name: str = "クラス"
    student_name: str = "氏　名"
    student_name_kana: str = "フリガナ"
    sex_type: str = "性別"


class JiyuStudents:
    def __init__(self, csv_file):
        self.cols = JiyuStuCols()
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
        return self.cols.student_name

    def join_target_col(self) -> str:
        return self.cols.exam_id


class ShiraishiExecutor:
    def __init__(self, cms_file, dng6_file, dng3_file, jyg_file) -> None:
        self._cms_cols = CmsDataCols()
        self._cms_data = CmsData(cms_file)
        self._dongri_data_6dic = DonguriAccount(dng6_file)
        self._dongri_data_3dic = DonguriAccount(dng3_file)
        self._jiyu_stu_cols = JiyuStuCols()
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
        _newbee_flgs = (self._cms_data.data[self._cms_cols.cur_school_year] == 0)
        self._cms_data.data = self._cms_data.data[_newbee_flgs]

    def __calc_dic_buying_type(self):
        """## 2. Calc Dictionary Buying Type (3dic or 6dic or None)

            - No1 - 6辞書
            - No2 - 3辞書
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
        __target_cols = [self._jiyu_stu_cols.exam_id, self._jiyu_stu_cols.course_name, self._jiyu_stu_cols.class_name, self._jiyu_stu_cols.student_name,
                         self._cms_cols.id, self._cms_cols.student_id, self._cms_cols.student_name, DICTYPE_COL_NAME]
        # 2021 sample data ==> __target_cols = ['テスト番号', '合格学科', 'クラス２', '出席番号', '氏　名', 'id', '学籍番号', '生徒名', '教科書タイトル', '副教材タイプ']
        self._merged_cms_jiyu = self._merged_cms_jiyu[__target_cols]

        # '副教材タイプ' fill na -> BuyingDicType.NULL
        self._merged_cms_jiyu[DICTYPE_COL_NAME].fillna(buying_dic_type.NULL, inplace=True)

        self._merged_cms_jiyu.to_csv('output-merged.csv')


    def __concat_donguri_acc_and_cmsjyg(self):
        """## CONCAT/MERGE - DONGURI ACC and CMS-JYG

            ### Note
            - アカウント数と生徒数を比べると、「6辞書、3辞書いずれも購入しない生徒」もいるようだ

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
        # Split CMS-JYG into DIC_6/DIC_3/DIC_NONE
        __merged_cms_jiyu_d6 = self._merged_cms_jiyu[self._merged_cms_jiyu[DICTYPE_COL_NAME] == buying_dic_type.DIC_6].copy()
        __merged_cms_jiyu_d3 = self._merged_cms_jiyu[self._merged_cms_jiyu[DICTYPE_COL_NAME] == buying_dic_type.DIC_3].copy()
        __merged_cms_jiyu_dN = self._merged_cms_jiyu[self._merged_cms_jiyu[DICTYPE_COL_NAME] == buying_dic_type.DIC_NONE].copy()
        __merged_cms_jiyu_NaN = self._merged_cms_jiyu[self._merged_cms_jiyu[DICTYPE_COL_NAME] == buying_dic_type.NULL].copy()

        # debug
        print()
        print(self._merged_cms_jiyu.columns)
        print(self._merged_cms_jiyu[DICTYPE_COL_NAME].unique())
        # debug

        _total_row = self._merged_cms_jiyu.shape[0]

        print(f'== 同時購入品として6辞書、3辞書いずれかを選んだ生徒')
        print(f'6辞書 購入者数 = {__merged_cms_jiyu_d6.shape[0]} / {_total_row} (準備済みアカウント数: {self._dongri_data_6dic.data.shape[0]})')
        print(f'3辞書 購入者数 = {__merged_cms_jiyu_d3.shape[0]} / {_total_row} (準備済みアカウント数: {self._dongri_data_3dic.data.shape[0]})')
        print()

        print(f'== 同時購入品として6辞書、3辞書いずれかも選んでない生徒')
        print(f'非購入者数 = {__merged_cms_jiyu_dN.shape[0]} / {_total_row}')
        print()

        print(f'== 文字化けなどを理由にCMS側に対応するデータが見つけられなかった生徒（手動作業）')
        print(f'手動オペレーション対象者数 = {__merged_cms_jiyu_NaN.shape[0]} / {_total_row}')
        print()

        # -------------------------------------------------
        # 2. RESULTS1 - Attach account info to students rows
        # dic 6
        _acc_rows_d6 = self._dongri_data_6dic.get_head(__merged_cms_jiyu_d6.shape[0])
        # -- concat cols は index が一致するものをつなぐので整えておく
        __merged_cms_jiyu_d6.reset_index(drop=True, inplace=True)
        _acc_rows_d6.reset_index(drop=True, inplace=True)
        _cms_jyg_acc_d6 = pd.concat([__merged_cms_jiyu_d6, _acc_rows_d6], axis=1) # axis=columns,1


        # dic 3
        _acc_rows_d3 = self._dongri_data_3dic.get_head(__merged_cms_jiyu_d3.shape[0])
        # -- concat cols は index が一致するものをつなぐので整えておく
        __merged_cms_jiyu_d3.reset_index(drop=True, inplace=True)
        _acc_rows_d3.reset_index(drop=True, inplace=True)
        _cms_jyg_acc_d3 = pd.concat([__merged_cms_jiyu_d3, _acc_rows_d3], axis=1) # axis=columns,1

        # dic None
        # -- 不要

        # Concat vertically
        self.cms_jyg_acc = pd.concat([_cms_jyg_acc_d6, _cms_jyg_acc_d3], axis=0) # axis=rows:0
        self.cms_jyg_acc.fillna(value="アカウント不足", inplace=True)

        # -------------------------------------------------
        # 3. RESULTS2 - No Buying data
        self.cms_jyg_no_buyer = __merged_cms_jiyu_dN.copy()
        self.cms_jyg_no_buyer.reset_index(drop=True, inplace=True)

        # -------------------------------------------------
        # 4. RESULTS3 - To Manual Operate Data
        # CMSデータとマッチングできなかった jyg データ
        _jyg_target_cols = [
            self._jiyu_stu_cols.exam_id,
            self._jiyu_stu_cols.course_name,
            self._jiyu_stu_cols.class_name,
            self._jiyu_stu_cols.student_name
        ]
        self.jyg_manual_operate = __merged_cms_jiyu_NaN[_jyg_target_cols].copy()
        # 2021 sample --> self.jyg_manual_operate = __merged_cms_jiyu_NaN[['テスト番号', '合格学科', 'クラス２', '出席番号', '氏　名']].copy()
        self.jyg_manual_operate.reset_index(drop=True, inplace=True)

        # jyg データとマッチングしていない CMSデータ（新1年のみ、前処理で抽出済み）
        # -- マッチング成功した '学籍番号' 一覧取得
        _successfully_matched_ids = []
        _successfully_matched_ids.extend(self.cms_jyg_acc[self._cms_cols.student_id].values)
        _successfully_matched_ids.extend(self.cms_jyg_no_buyer[self._cms_cols.student_id].values)
        # -- これを含まないCMSデータだけ取得
        self._cms_newbee_unmatched = self._cms_data.data[~self._cms_data.data[self._cms_cols.student_id].isin(_successfully_matched_ids)].copy()
        __target_cols = [
            self._cms_cols.id,
            self._cms_cols.student_id,
            self._cms_cols.student_name,
            DICTYPE_COL_NAME
        ]
        self._cms_newbee_unmatched = self._cms_newbee_unmatched[__target_cols]
        self._cms_newbee_unmatched.reset_index(drop=True, inplace=True)

    def __export(self):
        """## EXPORT

            ### Note
                - 下記を分けて出力する
                - CMSに対応データが見つかったデータ（自動算出 **成功**）
                - 学籍番号入力ミスなどにより、CMSに対応データが見つからなかったデータ（自動算出 **失敗**）
                    - ★手動オペレーションに利用
                - 残りのアカウント情報 - 6辞書
                - 残りのアカウント情報 - 3辞書

            ### Fmt
                - Excel File: `学生情報・DONGURIアカウント情報紐付け結果一覧.xlsx`
                    - Sheet: `購入者` - RESULTS1
                    - Sheet: `非購入者` - RESULTS2
                - Excel File: `DONGURIアカウント情報紐付けに失敗した学生一覧.xlsx`
                    - Sheet: `生徒一覧` - RESULTS3
                    - Sheet: `購入情報-マッチング候補` - RESULTS3
                - Excel File: `DONGURI残りのアカウント一覧.xlsx`
                    - Sheet: `6辞書アカウント` - RESULTS4
                    - Sheet: `3辞書アカウント` - RESULTS4

            https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_excel.html?highlight=to_excel#pandas.DataFrame.to_excel
        """
        with pd.ExcelWriter(FP_RESULT) as writer:
            self.cms_jyg_acc.to_excel(writer, sheet_name=SHN_RESULT_BUYER, index=False)
            self.cms_jyg_no_buyer.to_excel(writer, sheet_name=SHN_RESULT_NO_BUYER, index=False)

        with pd.ExcelWriter(FP_FAILED_STUDENTS) as writer:
            self.jyg_manual_operate.to_excel(writer, sheet_name=SHN_FAILED_STUDENTS_JYG, index=False)
            self._cms_newbee_unmatched.to_excel(writer, sheet_name=SHN_FAILED_STUDENTS_CMS, index=False)

        with pd.ExcelWriter(FP_REST_DONGURI_ACC) as writer:
            self._dongri_data_6dic.get_rest_of().to_excel(writer, sheet_name=SHN_REST_DONGURI_ACC_6DIC, index=False)
            self._dongri_data_3dic.get_rest_of().to_excel(writer, sheet_name=SHN_REST_DONGURI_ACC_3DIC, index=False)



class StatsManager:
    """statistics manager

        - 統計情報を管理する
        - 出力する
    """
    def __init__(self):
        self._cms_cols = CmsDataCols()
        self._stats = {}

    def load_cms_data(self, cms_path: str):
        """load cms data

            - cms_path: str
                - cms data path
        """
        cms_file = open(cms_path, 'rb')
        cms_data_obj = CmsData(cms_file)
        self._stats['cms_path'] = cms_path
        self._stats['cms_data'] = cms_data_obj.data

    def get_stats(self) -> dict:
        """get statistics

            - return: dict
                - statistics
        """
        return self._stats

    def aggregate_cms_data(self):
        """aggregate cms data

            - cms_data: pd.DataFrame
                - cms data
        """
        cms_data = self._stats['cms_data']

        # --------------------------------------------------
        # Contents
        # 1. 全生徒数（学籍番号ユニーク） - S1
        # 2. 全生徒数（名前ユニーク） - S2
        # 3. 6辞書購入者総数 - A
        # 4. 3辞書購入者総数 - B
        # 5. 辞書非購入者総数（S1 - (A+B) and S2 - (A+B)）
        # 6. 辞書非購入者総数（購入履歴から抽出ロジックを実装ーアプリで使ってるもの）
        # --------------------------------------------------
        # 1. 全生徒数（学籍番号ユニーク） - S1
        unique_sid_arr = cms_data[self._cms_cols.student_id].unique()
        self._stats['S1'] = len(unique_sid_arr)
        # print('[INFO] 全生徒数（学籍番号ユニーク）: {}'.format(len(unique_sid_arr)))

        # 2. 全生徒数（名前ユニーク） - S2
        unique_sname_arr = cms_data[self._cms_cols.student_name].unique()
        self._stats['S2'] = len(unique_sname_arr)
        # print('[INFO] 全生徒数（名前ユニーク）: {}'.format(len(unique_sname_arr)))

        # 2.5. 全生徒数（学籍番号＆名前ユニーク） - S3
        _df = cms_data.copy()
        unique_id_name_arr = _df.drop_duplicates(subset=[self._cms_cols.student_id, self._cms_cols.student_name])
        self._stats['S3'] = unique_id_name_arr.shape[0]
        print('[INFO] 全生徒数（学籍番号＆名前ユニーク）: {}'.format(unique_id_name_arr.shape[0]))

        # 3. 6辞書購入者総数 - A
        dic6_orders = cms_data[cms_data[self._cms_cols.prod_name].str.contains(PROD_NAME_DIC6, regex=False)]
        self._stats['A'] = dic6_orders.shape[0]
        print('[INFO] 6辞書購入者総数: {}'.format(dic6_orders.shape[0]))

        # 4. 3辞書購入者総数 - B
        dic3_orders = cms_data[cms_data[self._cms_cols.prod_name].str.contains(PROD_NAME_DIC3, regex=False)]
        self._stats['B'] = dic3_orders.shape[0]
        print('[INFO] 3辞書購入者総数: {}'.format(dic3_orders.shape[0]))

        # 5. 辞書非購入者総数（S1 - (A+B) and S2 - (A+B)）
        self._stats['S1_minus_A_plus_B'] = self._stats['S1'] - (self._stats['A'] + self._stats['B'])
        self._stats['S2_minus_A_plus_B'] = self._stats['S2'] - (self._stats['A'] + self._stats['B'])
        self._stats['S3_minus_A_plus_B'] = self._stats['S3'] - (self._stats['A'] + self._stats['B'])

        # print('[INFO] 辞書非購入者総数: S1 - (A+B): {}'.format(self._stats['S1_minus_A_plus_B']))
        # print('[INFO] 辞書非購入者総数: S2 - (A+B): {}'.format(self._stats['S2_minus_A_plus_B']))
        print('[INFO] 辞書非購入者総数: S3 - (A+B): {}'.format(self._stats['S3_minus_A_plus_B']))

        # 6. 辞書非購入者総数（購入履歴から抽出ロジックを実装ーアプリで使ってるもの）
        print("[INFO] 辞書非購入者総数（購入履歴から抽出ロジックを実装ーアプリで使ってるもの） - NotImplemented")

