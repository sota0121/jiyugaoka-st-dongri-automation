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


if __name__ == "__main__":
    tb()