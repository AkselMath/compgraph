import examples
from click.testing import CliRunner
from pathlib import PosixPath
import typing as tp
from operator import itemgetter


docs = [
    {'doc_id': 1, 'text': 'hello, my little WORLD'},
    {'doc_id': 2, 'text': 'Hello, my little little hell'}
]

expected = [
    {'count': 1, 'text': 'hell'},
    {'count': 1, 'text': 'world'},
    {'count': 2, 'text': 'hello'},
    {'count': 2, 'text': 'my'},
    {'count': 3, 'text': 'little'}
]


def list2str(data: list[dict[str, tp.Any]]) -> str:
    data_str = [str(i) for i in data]
    return "\n".join(data_str)


def str2dict(data: str) -> list[dict[str, tp.Any]]:
    data_list = data.split("\n")
    return [eval(i) for i in data_list if i != ""]


def test_run_word_count(tmp_path: PosixPath) -> None:
    d = tmp_path / "sub"
    d.mkdir()
    input_filepath = d / "docs.txt"
    output_filepath = d / "expected.txt"

    docs_str = list2str(docs)

    input_filepath.write_text(docs_str, encoding="utf-8")
    assert str2dict(input_filepath.read_text(encoding="utf-8")) == docs

    runner = CliRunner()
    runner.invoke(
        examples.run_word_count.main,
        ["--input_filepath", str(input_filepath),
         "--output_filepath", str(output_filepath)]
    )

    result = str2dict(output_filepath.read_text(encoding="utf-8"))
    assert sorted(result, key=itemgetter('count', 'text')) == expected


def test_word_count_multiple_call(tmp_path: PosixPath) -> None:
    d = tmp_path / "sub"
    d.mkdir()
    input_filepath = d / "docs.txt"
    output_filepath = d / "expected.txt"

    docs_str = list2str(docs)

    input_filepath.write_text(docs_str, encoding="utf-8")
    assert str2dict(input_filepath.read_text(encoding="utf-8")) == docs

    runner = CliRunner()
    runner.invoke(
        examples.run_word_count.main,
        ["--input_filepath", str(input_filepath),
         "--output_filepath", str(output_filepath)]
    )

    result = str2dict(output_filepath.read_text(encoding="utf-8"))
    assert sorted(result, key=itemgetter('count', 'text')) == expected

    runner = CliRunner()
    runner.invoke(
        examples.run_word_count.main,
        ["--input_filepath", str(input_filepath),
         "--output_filepath", str(output_filepath)]
    )

    result = str2dict(output_filepath.read_text(encoding="utf-8"))
    assert sorted(result, key=itemgetter('count', 'text')) == expected
