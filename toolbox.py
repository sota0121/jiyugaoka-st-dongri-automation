from pathlib import Path

import chardet
import click
import pandas as pd
import pdfkit

from src.executor import ShiraishiExecutor
from src.executor import StatsManager


@click.group(name="tb", help="Toolbox cli")
def tb():
    pass

@tb.command(name="to-utf8", help="Convert a csv file to utf8")
@click.option("--input", "-i", type=str, help="Input file", required=True)
@click.option("--output", "-o", type=str, help="Output file (default: input.utf8.csv)")
def to_utf8(input: str, output: str):
    """
    Convert a csv file to utf8
    """
    encoding = ""
    with open(input, "rb") as f:
        result = chardet.detect(f.read())
        click.echo(f"input file encoding --> {result}")
        encoding = result["encoding"]

    if encoding == "utf-8":
        click.echo("input file is already utf8")
        return

    df = pd.read_csv(input, encoding=encoding)
    output_path = output if output is not None else ".".join([input, "utf8.csv"])
    df.to_csv(output_path, encoding="utf-8", index=False)
    click.echo("Done")


@tb.command(name="export-list", help="Export the result of matching list to pdf")
@click.option("--input", "-i", type=str, help="Input file", required=True)
def export_list(input: str):
    """
    Export the result of matching list to pdf
    """
    df = pd.read_csv(input)
    df.to_html("list.html")
    pdfkit.from_file("list.html", "list.pdf")
    click.echo("Done")


@tb.command(name="test")
@click.option("--input", "-i", type=str, nargs=2, help="Input file", required=True)
def test(input: str):
    """
    Test
    """
    click.echo(input)
    a, b = input
    pdfkit.from_file(["list.html", "list2.html"], "list-2.pdf")
    click.echo("Done")


@tb.command(name='tmp-cnv')
@click.option("--input", "-i", type=str, help="Input file", required=True)
def tmp_cnv(input: str):
    """
    Transform 2022 fmt --> 2021 fmt
    DONGURI account csv

        This is Temporary function.
    """
    # load
    click.echo(f"load ... {input}")
    df_2022fmt = pd.read_csv(input)

    # split 6dic/3dic
    df_2022fmt_6dic = df_2022fmt[df_2022fmt['備考'].str.contains('ジーニアス５辞書')].copy()
    df_2022fmt_3dic = df_2022fmt[df_2022fmt['備考'].str.contains('ジーニアス英和/和英')].copy()

    # extract columns to export
    target_cols_2021fmt = ['ユーザー名', 'グループ名', '一時パスワード']
    df_2021fmt_6dic = df_2022fmt_6dic[target_cols_2021fmt].copy()
    df_2021fmt_3dic = df_2022fmt_3dic[target_cols_2021fmt].copy()

    # export
    odir = Path(input).parent
    fstem = Path(input).stem

    df_2021fmt_6dic.to_excel(odir / f"{fstem}_6dic.xlsx", index=False)
    df_2021fmt_3dic.to_excel(odir / f"{fstem}_3dic.xlsx", index=False)

    click.echo("Done")


@tb.command(name='emulator')
@click.option("--input-cms", "-ic", type=str, help="Input file - CMS Data (CSV/UTF-8)", required=True)
@click.option("--input-dic6", "-id6", type=str, help="Input file - Dict Accounts 6dic (xlsx)", required=True)
@click.option("--input-dic3", "-id3", type=str, help="Input file - Dict Accounts 3dic (xlsx)", required=True)
@click.option("--input-schooltest", "-ist", type=str, help="Input file - School Test Data (CSV/UTF-8)", required=True)
def emulator(input_cms: str, input_dic6: str, input_dic3: str, input_schooltest: str):
    """Streamlit App Emulator"""
    _cms_file = open(input_cms, "rb")
    _donguri6_file = open(input_dic6, "rb")
    _donguri3_file = open(input_dic3, "rb")
    _schooltest_file = open(input_schooltest, "rb")

    executor = ShiraishiExecutor(_cms_file, _donguri6_file, _donguri3_file, _schooltest_file)
    click.echo(f"executor created")
    click.echo(f"start to execute main process")
    executor.main_func()
    click.echo("Done")


@tb.command(name='stats', help="Show statistics of the input file (rakubuy order, CSV/UTF8)")
@click.option("--input", "-i", type=str, help="Input file", required=True)
def stats(input: str):
    """
    Show statistics of the input file (rakubuy order, CSV/UTF8)
    """
    stats_manager = StatsManager()
    stats_manager.load_cms_data(input)
    stats_manager.aggregate_cms_data()

    stats_result = stats_manager.get_stats()


if __name__ == "__main__":
    tb()