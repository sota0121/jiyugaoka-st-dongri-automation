"""streamlit app"""

from pathlib import Path
import time

import pandas as pd
import streamlit as st

from src.executor import ShiraishiExecutor
from src.executor import OUT_DIR, FP_RESULT, FN_RESULT, FP_FAILED_STUDENTS, FN_FAILED_STUDENTS, FP_REST_DONGURI_ACC, FN_REST_DONGURI_ACC


# FUNCTIONS
def cleanup_result_files():
    _files = list(Path(OUT_DIR).glob('*'))
    for _f in _files:
        _f.unlink(missing_ok=True)


# DISPLAY

st.title('Students and Accounts Linking Automation')
st.header('File Upload')

st.subheader('1. CMS DATA (CSV/SHIFT-JIS)')
_cms_file = st.file_uploader(label="Choose a file", key="cms_data")

st.subheader('2. DONGURI DATA (EXCEL/SHIFT-JIS) - 5辞書')
_donguri5_file = st.file_uploader(label="Choose a file", key="d5_data")

st.subheader('3. DONGURI DATA (EXCEL/SHIFT-JIS) - 2辞書')
_donguri2_file = st.file_uploader(label="Choose a file", key="d2_data")

st.subheader('4. STUDENT DATA from School Test (CSV/UTF-8)')
_jyg_file = st.file_uploader(label="Choose a file", key="jyg_data")


st.header('Execution')
executable = False
pressed = False
if (_cms_file is not None) and (_donguri5_file is not None) and (
        _donguri2_file is not None) and (_jyg_file is not None):
    executable = True

if executable is True:
    executor = ShiraishiExecutor(_cms_file, _donguri5_file, _donguri2_file, _jyg_file)
    pressed = st.button(label="Execute", key="exec_main", on_click=executor.main_func)

if pressed == True:
    st.write('executed')
else:
    st.write('not yet')



st.header('Download Files')

st.button(label="Clear Result", key="clear_result", on_click=cleanup_result_files)

# RESULT
downloadable_result = False
if (executable) and (Path(FP_RESULT).exists()):
    downloadable_result = True
if downloadable_result:
    with open(FP_RESULT, 'rb') as f:
        st.download_button(label=f'download {FN_RESULT}',
                           data=f,
                           file_name=FN_RESULT,
                           key=FN_RESULT)

# FAILED STUDENTS
downloadable_failed_st = False
if (executable) and (Path(FP_FAILED_STUDENTS).exists()):
    downloadable_failed_st = True
if downloadable_failed_st:
    with open(FP_FAILED_STUDENTS, 'rb') as f:
        st.download_button(label=f'download {FN_FAILED_STUDENTS}',
                           data=f,
                           file_name=FN_FAILED_STUDENTS,
                           key=FN_FAILED_STUDENTS)

# REST DONGURI ACCOUNTS
downloadable_rest_acc = False
if (executable) and (Path(FP_REST_DONGURI_ACC).exists()):
    downloadable_rest_acc = True
if downloadable_rest_acc:
    with open(FP_REST_DONGURI_ACC, 'rb') as f:
        st.download_button(label=f'download {FN_REST_DONGURI_ACC}',
                           data=f,
                           file_name=FN_REST_DONGURI_ACC,
                           key=FN_REST_DONGURI_ACC)
