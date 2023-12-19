import examples
from click.testing import CliRunner

from pathlib import PosixPath
from pytest import approx
from operator import itemgetter
import typing as tp


lengths = [
    {'start': [37.84870228730142, 55.73853974696249], 'end': [37.8490418381989, 55.73832445777953],
     'edge_id': 8414926848168493057},
    {'start': [37.524768467992544, 55.88785375468433], 'end': [37.52415172755718, 55.88807155843824],
     'edge_id': 5342768494149337085},
    {'start': [37.56963176652789, 55.846845586784184], 'end': [37.57018438540399, 55.8469259692356],
     'edge_id': 5123042926973124604},
    {'start': [37.41463478654623, 55.654487907886505], 'end': [37.41442892700434, 55.654839486815035],
     'edge_id': 5726148664276615162},
    {'start': [37.584684155881405, 55.78285809606314], 'end': [37.58415022864938, 55.78177368734032],
     'edge_id': 451916977441439743},
    {'start': [37.736429711803794, 55.62696328852326], 'end': [37.736344216391444, 55.626937723718584],
     'edge_id': 7639557040160407543},
    {'start': [37.83196756616235, 55.76662947423756], 'end': [37.83191015012562, 55.766647034324706],
     'edge_id': 1293255682152955894},
]

times = [
    {'leave_time': '20171020T112238.723000', 'enter_time': '20171020T112237.427000',
     'edge_id': 8414926848168493057},
    {'leave_time': '20171011T145553.040000', 'enter_time': '20171011T145551.957000',
     'edge_id': 8414926848168493057},
    {'leave_time': '20171020T090548.939000', 'enter_time': '20171020T090547.463000',
     'edge_id': 8414926848168493057},
    {'leave_time': '20171024T144101.879000', 'enter_time': '20171024T144059.102000',
     'edge_id': 8414926848168493057},
    {'leave_time': '20171022T131828.330000', 'enter_time': '20171022T131820.842000',
     'edge_id': 5342768494149337085},
    {'leave_time': '20171014T134826.836000', 'enter_time': '20171014T134825.215000',
     'edge_id': 5342768494149337085},
    {'leave_time': '20171010T060609.897000', 'enter_time': '20171010T060608.344000',
     'edge_id': 5342768494149337085},
    {'leave_time': '20171027T082600.201000', 'enter_time': '20171027T082557.571000',
     'edge_id': 5342768494149337085}
]

expected = [
    {'weekday': 'Fri', 'hour': 8, 'speed': approx(62.2322, 0.001)},
    {'weekday': 'Fri', 'hour': 9, 'speed': approx(78.1070, 0.001)},
    {'weekday': 'Fri', 'hour': 11, 'speed': approx(88.9552, 0.001)},
    {'weekday': 'Sat', 'hour': 13, 'speed': approx(100.9690, 0.001)},
    {'weekday': 'Sun', 'hour': 13, 'speed': approx(21.8577, 0.001)},
    {'weekday': 'Tue', 'hour': 6, 'speed': approx(105.3901, 0.001)},
    {'weekday': 'Tue', 'hour': 14, 'speed': approx(41.5145, 0.001)},
    {'weekday': 'Wed', 'hour': 14, 'speed': approx(106.4505, 0.001)}
]


def list2str(data: list[dict[str, tp.Any]]) -> str:
    data_str = [str(i) for i in data]
    return "\n".join(data_str)


def str2dict(data: str) -> list[dict[str, tp.Any]]:
    data_list = data.split("\n")
    return [eval(i) for i in data_list if i != ""]


def test_pmi(tmp_path: PosixPath) -> None:
    d = tmp_path / "sub"
    d.mkdir()
    filepath_travel_time = d / "travel_time.txt"
    filepath_edge_length = d / "edge_length.txt"
    output_filepath = d / "expected.txt"

    times_str = list2str(times)
    lengths_str = list2str(lengths)

    filepath_travel_time.write_text(times_str, encoding="utf-8")
    filepath_edge_length.write_text(lengths_str, encoding="utf-8")

    assert str2dict(filepath_travel_time.read_text(encoding="utf-8")) == times
    assert str2dict(filepath_edge_length.read_text(encoding="utf-8")) == lengths

    runner = CliRunner()
    runner.invoke(
        examples.run_yandex_maps.main,
        ["--filepath_travel_time", str(filepath_travel_time),
         "--filepath_edge_length", str(filepath_edge_length),
         "--output_filepath", str(output_filepath)]
    )

    result = str2dict(output_filepath.read_text(encoding="utf-8"))
    assert sorted(result, key=itemgetter('weekday', "hour")) == expected


def test_pmi_multiple_call(tmp_path: PosixPath) -> None:
    d = tmp_path / "sub"
    d.mkdir()
    filepath_travel_time = d / "travel_time.txt"
    filepath_edge_length = d / "edge_length.txt"
    output_filepath = d / "expected.txt"

    times_str = list2str(times)
    lengths_str = list2str(lengths)

    filepath_travel_time.write_text(times_str, encoding="utf-8")
    filepath_edge_length.write_text(lengths_str, encoding="utf-8")

    assert str2dict(filepath_travel_time.read_text(encoding="utf-8")) == times
    assert str2dict(filepath_edge_length.read_text(encoding="utf-8")) == lengths

    runner = CliRunner()
    runner.invoke(
        examples.run_yandex_maps.main,
        ["--filepath_travel_time", str(filepath_travel_time),
         "--filepath_edge_length", str(filepath_edge_length),
         "--output_filepath", str(output_filepath)]
    )

    result = str2dict(output_filepath.read_text(encoding="utf-8"))
    assert sorted(result, key=itemgetter('weekday', "hour")) == expected

    runner = CliRunner()
    runner.invoke(
        examples.run_yandex_maps.main,
        ["--filepath_travel_time", str(filepath_travel_time),
         "--filepath_edge_length", str(filepath_edge_length),
         "--output_filepath", str(output_filepath)]
    )

    result = str2dict(output_filepath.read_text(encoding="utf-8"))
    assert sorted(result, key=itemgetter('weekday', "hour")) == expected
