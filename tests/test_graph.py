from compgraph import graph, external_sort, operations
from pathlib import PosixPath
import typing as tp


def list2str(data: list[dict[str, tp.Any]]) -> str:
    data_str = [str(i) for i in data]
    return "\n".join(data_str)


def str2dict(data: str) -> list[dict[str, tp.Any]]:
    data_list = data.split("\n")
    return [eval(i) for i in data_list if i != ""]


def test_graph_from_file(tmp_path: PosixPath) -> None:
    d = tmp_path / "sub"
    d.mkdir()
    input_filepath = d / "docs.txt"

    docs = [
        {'doc_id': 1, 'text': 'hello, my little WORLD'},
        {'doc_id': 2, 'text': 'Hello, my little little hell'}
    ]

    docs_str = list2str(docs)

    input_filepath.write_text(docs_str, encoding="utf-8")
    assert str2dict(input_filepath.read_text(encoding="utf-8")) == docs

    gr = graph.Graph.graph_from_file('name', operations.parser)
    result = [row for row in gr.run(name=lambda: str(input_filepath))]

    assert result == docs


def test_graph_from_iter() -> None:
    docs = [
        {'doc_id': 1, 'text': 'hello, my little WORLD'},
        {'doc_id': 2, 'text': 'Hello, my little little hell'}
    ]

    gr = graph.Graph.graph_from_iter('name')
    result = [row for row in gr.run(name=lambda: iter(docs))]

    assert result == docs


def test_graph_from_(tmp_path: PosixPath) -> None:
    docs = [
        {'doc_id': 1, 'text': 'hello, my little WORLD'},
        {'doc_id': 2, 'text': 'Hello, my little little hell'}
    ]

    gr = graph.Graph.graph_from_('name')
    result = [row for row in gr.run(name=lambda: iter(docs))]

    assert result == docs

    d = tmp_path / "sub"
    d.mkdir()
    input_filepath = d / "docs.txt"

    docs_str = list2str(docs)

    input_filepath.write_text(docs_str, encoding="utf-8")
    assert str2dict(input_filepath.read_text(encoding="utf-8")) == docs

    gr = graph.Graph.graph_from_('name', operations.parser)
    result = [row for row in gr.run(name=lambda: str(input_filepath))]

    assert result == docs


def test_graph_push() -> None:
    gr = graph.Graph.graph_from_('name')
    gr.push_operations(operations.Map(operations.DummyMapper()))

    assert isinstance(gr.operations[0][0], operations.Map)
    assert isinstance(gr.operations[0][0].mapper, operations.DummyMapper)


def test_graph_push_map() -> None:
    gr = graph.Graph.graph_from_('name').map(operations.DummyMapper())

    assert isinstance(gr.operations[0][0], operations.Map)
    assert isinstance(gr.operations[0][0].mapper, operations.DummyMapper)


def test_graph_push_reduce() -> None:
    gr = graph.Graph.graph_from_('name').reduce(operations.FirstReducer(), ['temp'])

    assert isinstance(gr.operations[0][0], operations.Reduce)
    assert isinstance(gr.operations[0][0].reducer, operations.FirstReducer)


def test_graph_push_join() -> None:
    join_graph = graph.Graph.graph_from_('name').map(operations.DummyMapper())
    gr = graph.Graph.graph_from_('name').join(operations.InnerJoiner(), join_graph, ['temp'])

    assert isinstance(gr.operations[0][0], operations.Join)
    assert isinstance(gr.operations[0][0].joiner, operations.InnerJoiner)


def test_graph_push_sort() -> None:
    gr = graph.Graph.graph_from_('name').sort(['temp'])
    assert isinstance(gr.operations[0][0], external_sort.ExternalSort)


def test_graph_multiple_call() -> None:
    docs = [
        {'doc_id': 1, 'text': 'hello, my little WORLD'},
        {'doc_id': 2, 'text': 'Hello, my little little hell'}
    ]

    gr = graph.Graph.graph_from_('name')

    result = [row for row in gr.run(name=lambda: iter(docs))]
    assert result == docs

    result = [row for row in gr.run(name=lambda: iter(docs))]
    assert result == docs
