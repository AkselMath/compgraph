import copy
import dataclasses
import typing as tp

import pytest

import math
from datetime import datetime

from pytest import approx

from compgraph import operations as ops


class _Key:
    def __init__(self, *args: str) -> None:
        self._items = args

    def __call__(self, d: tp.Mapping[str, tp.Any]) -> tuple[str, ...]:
        return tuple(str(d.get(key)) for key in self._items)


@dataclasses.dataclass
class MapCase:
    mapper: ops.Mapper
    data: list[ops.TRow]
    ground_truth: list[ops.TRow]
    cmp_keys: tuple[str, ...]
    mapper_item: int = 0
    mapper_ground_truth_items: tuple[int, ...] = (0,)


MAP_CASES = [
    MapCase(
        mapper=ops.LogarithmOfRatioOfColumn(
            "frequency_column_in_document",
            "frequency_column_in_all_document",
            'pmi'),
        data=[
            {'frequency_column_in_document': 6.4,
             'frequency_column_in_all_document': 1.15},
            {'frequency_column_in_document': 55.43,
             'frequency_column_in_all_document': 0.23},
            {'frequency_column_in_document': 23.23,
             'frequency_column_in_all_document': 412.234},
            {'frequency_column_in_document': 23.0,
             'frequency_column_in_all_document': 42.35},
        ],
        ground_truth=[
            {'pmi': approx(math.log(6.4 / 1.15), 0.001)},
            {'pmi': approx(math.log(55.43 / 0.23), 0.001)},
            {'pmi': approx(math.log(23.23 / 412.234), 0.001)},
            {'pmi': approx(math.log(23.0 / 42.35), 0.001)},
        ],
        cmp_keys=('frequency_column_in_document',
                  'frequency_column_in_all_document')
    ),
    MapCase(
        mapper=ops.MoreTwice("first_col"),
        data=[
            {'first_col': 7, 'second_col': 1.15},
            {'first_col': 1, 'second_col': 1.15},
            {'first_col': 4, 'second_col': 1.15},
            {'first_col': 5, 'second_col': 1.15},
            {'first_col': 1, 'second_col': 1.15},
            {'first_col': 10, 'second_col': 1.15},
            {'first_col': -10, 'second_col': 1.15}
        ],
        ground_truth=[
            {'first_col': 7, 'second_col': 1.15},
            {'first_col': 4, 'second_col': 1.15},
            {'first_col': 5, 'second_col': 1.15},
            {'first_col': 10, 'second_col': 1.15},
        ],
        cmp_keys=('first_col', 'second_col')
    ),
    MapCase(
        mapper=ops.AddDummyColumn("new_col"),
        data=[
            {'first_col': 7, 'second_col': 1.15},
            {'first_col': 1, 'second_col': 1.15},
            {'first_col': 4, 'second_col': 1.15},
            {'first_col': 5, 'second_col': 1.15},
            {'first_col': 1, 'second_col': 1.15},
            {'first_col': 10, 'second_col': 1.15},
            {'first_col': -10, 'second_col': 1.15}
        ],
        ground_truth=[
            {'first_col': 7, 'second_col': 1.15, "new_col": 1},
            {'first_col': 1, 'second_col': 1.15, "new_col": 1},
            {'first_col': 4, 'second_col': 1.15, "new_col": 1},
            {'first_col': 5, 'second_col': 1.15, "new_col": 1},
            {'first_col': 1, 'second_col': 1.15, "new_col": 1},
            {'first_col': 10, 'second_col': 1.15, "new_col": 1},
            {'first_col': -10, 'second_col': 1.15, "new_col": 1}
        ],
        cmp_keys=('first_col', 'second_col')
    ),
    MapCase(
        mapper=ops.DeleteDummyColumn("new_col"),
        data=[
            {'first_col': 7, 'second_col': 1.15, "new_col": 1},
            {'first_col': 1, 'second_col': 1.15, "new_col": 1},
            {'first_col': 4, 'second_col': 1.15, "new_col": 1},
            {'first_col': 5, 'second_col': 1.15, "new_col": 1},
            {'first_col': 1, 'second_col': 1.15, "new_col": 1},
            {'first_col': 10, 'second_col': 1.15, "new_col": 1},
            {'first_col': -10, 'second_col': 1.15, "new_col": 1}
        ],
        ground_truth=[
            {'first_col': 7, 'second_col': 1.15},
            {'first_col': 1, 'second_col': 1.15},
            {'first_col': 4, 'second_col': 1.15},
            {'first_col': 5, 'second_col': 1.15},
            {'first_col': 1, 'second_col': 1.15},
            {'first_col': 10, 'second_col': 1.15},
            {'first_col': -10, 'second_col': 1.15}
        ],
        cmp_keys=('first_col', 'second_col')
    ),
    MapCase(
        mapper=ops.MoreFourCharacters("first_col"),
        data=[
            {'first_col': "asfsdggsaf44", 'second_col': 1.15, "new_col": 1},
            {'first_col': "asfsdggsaf44", 'second_col': 1.15, "new_col": 1},
            {'first_col': "ee", 'second_col': 1.15, "new_col": 1},
            {'first_col': "asfsdggsaf44", 'second_col': 1.15, "new_col": 1},
            {'first_col': "yy", 'second_col': 1.15, "new_col": 1},
            {'first_col': "asfsdggsaf44", 'second_col': 1.15, "new_col": 1},
            {'first_col': "211", 'second_col': 1.15, "new_col": 1}
        ],
        ground_truth=[
            {'first_col': "asfsdggsaf44", 'second_col': 1.15, "new_col": 1},
            {'first_col': "asfsdggsaf44", 'second_col': 1.15, "new_col": 1},
            {'first_col': "asfsdggsaf44", 'second_col': 1.15, "new_col": 1},
            {'first_col': "asfsdggsaf44", 'second_col': 1.15, "new_col": 1},
        ],
        cmp_keys=('first_col', 'second_col')
    ),
    MapCase(
        mapper=ops.ConvertToDatetime(["leave_time"], "%Y%m%dT%H%M%S.%f"),
        data=[
            {'leave_time': '20171020T112238.723000',
             'edge_id': 8414926848168493057},
            {'leave_time': '20171011T145553.040000',
             'edge_id': 8414926848168493057},
            {'leave_time': '20171020T090548.939000',
             'edge_id': 8414926848168493057},
            {'leave_time': '20171024T144101.879000',
             'edge_id': 8414926848168493057}
        ],
        ground_truth=[
            {'leave_time': datetime(2017, 10, 20,
                                    11, 22,
                                    38, 723000),
             'edge_id': 8414926848168493057},
            {'leave_time': datetime(2017, 10, 11,
                                    14, 55,
                                    53, 40000),
             'edge_id': 8414926848168493057},
            {'leave_time': datetime(2017, 10, 20,
                                    9, 5,
                                    48, 939000),
             'edge_id': 8414926848168493057},
            {'leave_time': datetime(2017, 10, 24,
                                    14, 41,
                                    1, 879000),
             'edge_id': 8414926848168493057}
        ],
        cmp_keys=('leave_time', 'edge_id')
    ),
    MapCase(
        mapper=ops.CompHaversine(("start", "end"), "length", 6373),
        data=[
            {'start': [37.84870228730142, 55.73853974696249],
             'end': [37.8490418381989, 55.73832445777953],
             'edge_id': 8414926848168493057},
            {'start': [37.524768467992544, 55.88785375468433],
             'end': [37.52415172755718, 55.88807155843824],
             'edge_id': 5342768494149337085},
            {'start': [37.56963176652789, 55.846845586784184],
             'end': [37.57018438540399, 55.8469259692356],
             'edge_id': 5123042926973124604},
            {'start': [37.41463478654623, 55.654487907886505],
             'end': [37.41442892700434, 55.654839486815035],
             'edge_id': 5726148664276615162},
        ],
        ground_truth=[
            {"length": approx(0.03202388862587725, 0.001),
             'edge_id': 8414926848168493057},
            {"length": approx(0.045464124356956986, 0.001),
             'edge_id': 5342768494149337085},
            {"length": approx(0.03564782207659032, 0.001),
             'edge_id': 5123042926973124604},
            {"length": approx(0.04118458581600073, 0.001),
             'edge_id': 5726148664276615162}
        ],
        cmp_keys=('length', 'edge_id')
    ),
    MapCase(
        mapper=ops.CompTimeDelta(("first_time", "second_time"), "time_delta"),
        data=[
            {'first_time': datetime(2017, 10, 20,
                                    11, 22,
                                    38, 723000),
             'second_time': datetime(2017, 11, 20,
                                     11, 22,
                                     38, 723000)},
            {'first_time': datetime(2017, 10, 11,
                                    14, 55,
                                    53, 40000),
             'second_time': datetime(2017, 11, 10,
                                     11, 22,
                                     38, 723000)},
            {'first_time': datetime(2017, 10, 20,
                                    9, 5,
                                    48, 939000),
             'second_time': datetime(2017, 10, 25,
                                     11, 50,
                                     38, 723000)},
            {'first_time': datetime(2017, 10, 20,
                                    11, 11,
                                    38, 723000),
             'second_time': datetime(2017, 10, 24,
                                     14, 41,
                                     1, 879000)},
        ],
        ground_truth=[
            {'first_time': datetime(2017, 10, 20,
                                    11, 22,
                                    38, 723000),
             'second_time': datetime(2017, 11, 20,
                                     11, 22,
                                     38, 723000),
             "time_delta": approx(744.0, 0.001)},
            {'first_time': datetime(2017, 10, 11,
                                    14, 55,
                                    53, 40000),
             'second_time': datetime(2017, 11, 10,
                                     11, 22,
                                     38, 723000),
             "time_delta": approx(716.4460230555557, 0.001)},
            {'first_time': datetime(2017, 10, 20,
                                    9, 5,
                                    48, 939000),
             'second_time': datetime(2017, 10, 25,
                                     11, 50,
                                     38, 723000),
             "time_delta": approx(122.74716222222222, 0.001)},
            {'first_time': datetime(2017, 10, 20,
                                    11, 11,
                                    38, 723000),
             'second_time': datetime(2017, 10, 24,
                                     14, 41,
                                     1, 879000),
             "time_delta": approx(99.48976555555556, 0.001)},
        ],
        cmp_keys=('first_col', 'second_col')
    ),
    MapCase(
        mapper=ops.GetWeekdayAndHour("time", "weekday", "hour"),
        data=[
            {'time': datetime(2017, 10, 20,
                              11, 22,
                              38, 723000)},
            {'time': datetime(2017, 10, 11,
                              14, 55,
                              53, 40000)},
        ],
        ground_truth=[
            {'weekday': 'Fri', 'hour': 11},
            {'weekday': 'Wed', 'hour': 14},
        ],
        cmp_keys=('first_col', 'second_col')
    ),
    MapCase(
        mapper=ops.AverageSpeed(("first_col", "second_col"), "speed"),
        data=[
            {"first_col": 323.114, "second_col": 1312.5353},
            {"first_col": 3235.14, "second_col": 0.323},
            {"first_col": 322.43114, "second_col": 1.3},
            {"first_col": 31.14, "second_col": 1312.5353},
        ],
        ground_truth=[
            {'speed': approx(323.114 / 1312.5353, 0.001)},
            {'speed': approx(3235.14 / 0.323, 0.001)},
            {'speed': approx(322.43114 / 1.3, 0.001)},
            {'speed': approx(31.14 / 1312.5353, 0.001)},
        ],
        cmp_keys=(('speed',))
    ),
]


@pytest.mark.parametrize('case', MAP_CASES)
def test_mapper(case: MapCase) -> None:
    mapper_data_row = copy.deepcopy(case.data[case.mapper_item])
    mapper_ground_truth_rows = [copy.deepcopy(case.ground_truth[i])
                                for i in case.mapper_ground_truth_items]
    key_func = _Key(*case.cmp_keys)

    mapper_result = case.mapper(mapper_data_row)
    assert isinstance(mapper_result, tp.Iterator)
    assert sorted(mapper_ground_truth_rows,
                  key=key_func) == sorted(mapper_result,
                                          key=key_func)

    result = ops.Map(case.mapper)(iter(case.data))
    assert isinstance(result, tp.Iterator)
    assert sorted(case.ground_truth,
                  key=key_func) == sorted(result,
                                          key=key_func)
