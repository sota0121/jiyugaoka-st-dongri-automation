from pathlib import Path

import chardet
import click
import pandas as pd
import pdfkit


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



if __name__ == "__main__":
    tb()