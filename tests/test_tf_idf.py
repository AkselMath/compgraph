import examples
from click.testing import CliRunner

from pytest import approx
from pathlib import PosixPath
from operator import itemgetter
import typing as tp


docs = [
    {'doc_id': 1, 'text': 'hello, little world'},
    {'doc_id': 2, 'text': 'little'},
    {'doc_id': 3, 'text': 'little little little'},
    {'doc_id': 4, 'text': 'little? hello little world'},
    {'doc_id': 5, 'text': 'HELLO HELLO! WORLD...'},
    {'doc_id': 6, 'text': 'world? world... world!!! WORLD!!! HELLO!!!'}
]

expected = [
    {'doc_id': 1, 'text': 'hello', 'tf_idf': approx(0.1351, 0.001)},
    {'doc_id': 1, 'text': 'world', 'tf_idf': approx(0.1351, 0.001)},

    {'doc_id': 2, 'text': 'little', 'tf_idf': approx(0.4054, 0.001)},

    {'doc_id': 3, 'text': 'little', 'tf_idf': approx(0.4054, 0.001)},

    {'doc_id': 4, 'text': 'hello', 'tf_idf': approx(0.1013, 0.001)},
    {'doc_id': 4, 'text': 'little', 'tf_idf': approx(0.2027, 0.001)},

    {'doc_id': 5, 'text': 'hello', 'tf_idf': approx(0.2703, 0.001)},
    {'doc_id': 5, 'text': 'world', 'tf_idf': approx(0.1351, 0.001)},

    {'doc_id': 6, 'text': 'world', 'tf_idf': approx(0.3243, 0.001)}
]


def list2str(data: list[dict[str, tp.Any]]) -> str:
    data_str = [str(i) for i in data]
    return "\n".join(data_str)


def str2dict(data: str) -> list[dict[str, tp.Any]]:
    data_list = data.split("\n")
    return [eval(i) for i in data_list if i != ""]


def test_tf_idf(tmp_path: PosixPath) -> None:
    d = tmp_path / "sub"
    d.mkdir()
    input_filepath = d / "docs.txt"
    output_filepath = d / "expected.txt"

    docs_str = list2str(docs)

    input_filepath.write_text(docs_str, encoding="utf-8")
    assert str2dict(input_filepath.read_text(encoding="utf-8")) == docs

    runner = CliRunner()
    runner.invoke(
        examples.run_inverted_index.main,
        ["--input_filepath", str(input_filepath),
         "--output_filepath", str(output_filepath)]
    )

    result = str2dict(output_filepath.read_text(encoding="utf-8"))

    assert sorted(result, key=itemgetter('doc_id', 'text')) == expected


def test_tf_idf_multiple_call(tmp_path: PosixPath) -> None:
    d = tmp_path / "sub"
    d.mkdir()
    input_filepath = d / "docs.txt"
    output_filepath = d / "expected.txt"

    docs_str = list2str(docs)

    input_filepath.write_text(docs_str, encoding="utf-8")
    assert str2dict(input_filepath.read_text(encoding="utf-8")) == docs

    runner = CliRunner()
    runner.invoke(
        examples.run_inverted_index.main,
        ["--input_filepath", str(input_filepath),
         "--output_filepath", str(output_filepath)]
    )

    result = str2dict(output_filepath.read_text(encoding="utf-8"))

    assert sorted(result, key=itemgetter('doc_id', 'text')) == expected

    runner = CliRunner()
    runner.invoke(
        examples.run_inverted_index.main,
        ["--input_filepath", str(input_filepath),
         "--output_filepath", str(output_filepath)]
    )

    result = str2dict(output_filepath.read_text(encoding="utf-8"))

    assert sorted(result, key=itemgetter('doc_id', 'text')) == expected
