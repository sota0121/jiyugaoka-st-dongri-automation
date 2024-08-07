# jiyugaoka-st-dongri-automation

【自由が丘高校専用】
1. 学校提供生徒情報-CMS生徒情報紐付け
2. 生徒情報にDONGURIアカウント情報をアタッチ

## Requirements

- pyenv
- pyenv-virtualenv
- Python 3.10.8 or later

## Setup

```bash
# install python with pyenv
pyenv install 3.10.8

# create virtual env
pyenv virtualenv 3.10.8 workenv

# activate virtual env
pyenv activate workenv

# If you want to deactivate virtual env
source deactivate
```


## Applications Overview

1. Streamlit App for Data Linking
2. Toolbox script


### Streamlit App

Run with command below.

```bash
# entry into virtual env
(venv) cd jiyugaoka-st-dongri-automation
(venv) streamlit run streamlit_app.py
```

Hosted here.

https://share.streamlit.io/sota0121/jiyugaoka-st-dongri-automation/main



### Toolbox script

CLI Application by `click`

```bash
# entry into virtual env
(venv) cd jiyugaoka-st-dongri-automation
(venv) python toolbox.py --help
```
