from abc import abstractmethod, ABC
import typing as tp

import heapq
import math
from datetime import datetime
from collections import Counter
from itertools import groupby, chain

TRow = dict[str, tp.Any]
TRowsIterable = tp.Iterable[TRow]
TRowsGenerator = tp.Generator[TRow, None, None]


class Operation(ABC):
    """Abstract class for all operations."""

    @abstractmethod
    def __call__(self,
                 rows: TRowsIterable,
                 *args: tp.Any,
                 **kwargs: tp.Any) -> TRowsGenerator:
        """Abstract method call for all operations.

        Args:
            rows: table rows as an iterator.

        Yields:
            yield table row.
        """
        pass


class Read(Operation):
    """Reads a table from a file.

        Attributes:
            filename: file name to read.
            parser: preprocessing a line from a file.
    """

    def __init__(self,
                 filename: str,
                 parser: tp.Callable[[str], TRow]) -> None:
        """Initializes the instance Read.

        Args:
            filename: file name to read.
            parser: preprocessing a line from a file.
        """
        self.name = filename
        self.parser = parser

    def __call__(self, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        """Reading from file.

        Yields:
            yield line from a file.
        """
        filename = kwargs[self.name]()
        with open(filename) as f:
            for line in f:
                yield self.parser(line)


class ReadIterFactory(Operation):
    """Reads a table from a iterator.

    Attributes:
        name: name of iterator.
    """

    def __init__(self, name: str) -> None:
        """Initializes the instance ReadIterFactory.

        Args:
            name: name of iterator.
        """
        self.name = name

    def __call__(self, *args: tp.Any, **kwargs: tp.Any) -> TRowsGenerator:
        """Reading from iterator kwargs[self.name]

        Yields:
            yield next form iterator (kwargs[self.name]).
        """
        for row in kwargs[self.name]():
            yield row


# Operations


class Mapper(ABC):
    """Base class for mappers."""

    @abstractmethod
    def __call__(self, row: TRow) -> TRowsGenerator:
        """Abstract method call for all mapper.

        Args:
            row: one table row.

        Yields:
            yield table row or rows.
        """
        pass


class Map(Operation):
    """Applies a mapper to each line.

    Attributes:
        mapper: сlass that applies to each row.
    """

    def __init__(self, mapper: Mapper) -> None:
        """Initializes the instance Map.

        Args:
            mapper: сlass that applies to each row.
        """
        self.mapper = mapper

    def __call__(self,
                 rows: TRowsIterable,
                 *args: tp.Any,
                 **kwargs: tp.Any) -> TRowsGenerator:
        """Applies a mapper to each line.

        Args:
            rows: table as an iterator.

        Yields:
            yield one table row.
        """
        for row in rows:
            for mapper_row in self.mapper(row):
                yield mapper_row


class Reducer(ABC):
    """Base class for reducers"""

    @abstractmethod
    def __call__(self,
                 group_key: tuple[str, ...],
                 rows: TRowsIterable) -> TRowsGenerator:
        """Abstract method call for all reducers.

        Args:
            group_key: name columns by which the table is grouped.
            rows: table rows.

        Yields:
            yield table row or rows.
        """
        pass


class Reduce(Operation):
    """Applies a reducers to table row groups.

    Attributes:
        reducer: сlass that applies to each table row groups.
        keys: name columns by which the table is grouped.

    """

    def __init__(self, reducer: Reducer, keys: tp.Sequence[str]) -> None:
        """Initializes the instance Reduce.

        Attributes:
            reducer: сlass that applies to each table row groups.
            keys: name columns by which the table is grouped.
        """
        self.reducer = reducer
        self.keys = keys

    def sort_func(self, x: TRow) -> list[tp.Any]:
        """Method for obtaining keys by which a table is grouped.

        Args:
            x: one table row.

        Returns:
            columns by which the table is grouped
        """
        return [x[i] for i in self.keys]

    def __call__(self,
                 rows: TRowsIterable,
                 *args: tp.Any,
                 **kwargs: tp.Any) -> TRowsGenerator:
        """Applies a reducers to table row groups.

        Args:
            rows: table as an iterator.

        Yields:
            yield table row.
        """
        for keys, group_rows in groupby(rows, key=self.sort_func):
            for reducer_row in self.reducer(tuple(self.keys), group_rows):
                yield reducer_row


class Joiner(ABC):
    """Base class for joiners.

    Attributes:
        _a_suffix: first suffix for names of identical table columns.
        _b_suffix: second suffix for names of identical table columns.
    """

    def __init__(self, suffix_a: str = '_1', suffix_b: str = '_2') -> None:
        """Initializes the instance Joiner.

        Args:
            _a_suffix: first suffix for names of identical table columns.
            _b_suffix: second suffix for names of identical table columns.
        """
        self._a_suffix = suffix_a
        self._b_suffix = suffix_b

    @staticmethod
    def is_empty(
            rows_a: TRowsIterable,
            rows_b: TRowsIterable
    ) -> tuple[tp.Optional[TRow], tp.Optional[TRow]]:
        """Static method checks two table iterators for emptyness.

        Args:
            rows_a: first table as an iterator.
            rows_b: second table as an iterator.

        Return:
            For rows_a and rows_b, if the iterator is not empty,
                then returns next(iterator), otherwise None.
        """
        row_a, row_b = None, None
        try:
            row_a = next(iter(rows_a))
        except StopIteration:
            pass
        try:
            row_b = next(iter(rows_b))
        except StopIteration:
            pass
        return row_a, row_b

    def mearge_table(self,
                     keys: tp.Sequence[str],
                     row_a: TRow,
                     row_b: TRow,
                     rows_a: TRowsIterable,
                     rows_b: TRowsIterable) -> TRowsGenerator:
        """Мethod joins two non-empty tables.

        Args:
            keys: names of the columns along which the join is performed.
            row_a: first table element rows_a.
            row_b: first table element rows_b.
            rows_a: first table as an iterator.
            rows_b: second table as an iterator.

        Yields:
            yield table rows.
        """
        list_rows_b = [row_b]
        for row in rows_b:
            list_rows_b.append(row)

        for left_row in chain(iter([row_a]), rows_a):
            for right_row in list_rows_b:
                temp_row = {}
                for column in left_row:
                    if column in right_row and column not in keys:
                        suffix = f'{column}{self._a_suffix}'
                        temp_row[suffix] = left_row[column]
                    else:
                        temp_row[column] = left_row[column]

                for column in right_row:
                    if column in left_row and column not in keys:
                        suffix = f'{column}{self._b_suffix}'
                        temp_row[suffix] = right_row[column]
                    else:
                        temp_row[column] = right_row[column]
                yield temp_row

    @abstractmethod
    def __call__(self,
                 keys: tp.Sequence[str],
                 rows_a: TRowsIterable,
                 rows_b: TRowsIterable) -> TRowsGenerator:
        """Abstract method for joins two tables.

        Args:
            keys: names of the columns along which the join is performed.
            rows_a: first table as an iterator.
            rows_b: second table as an iterator.

        Yields:
            yield table rows.
        """
        pass


class Join(Operation):
    """Groups two tables by keys and joins them.

    Attributes:
        keys: names of the columns along which the join is performed.
        joiner: table join type.
    """

    def __init__(self, joiner: Joiner, keys: tp.Sequence[str]):
        """Initializes the instance Join.

        Args:
            keys: names of the columns along which the join is performed.
            joiner: table join type.
        """
        self.keys = keys
        self.joiner = joiner

    @staticmethod
    def my_next(
            rows: tp.Iterator[tuple[tp.Any, TRowsIterable]]
    ) -> tuple[tp.Optional[list[tp.Any]], TRowsIterable]:
        """Wrapper for next.

        Args:
            rows: table as an iterator after grouping by keys.

        Return:
            Returns the grouping keys and table group if they exist,
                otherwise None.
        """
        try:
            left_keys, left_group_rows = next(rows)
            return left_keys, left_group_rows
        except StopIteration:
            return None, iter({})

    def sort_func(self, x: TRow) -> list[tp.Any]:
        """Method for obtaining keys by which a table is grouped.

        Args:
            x: one table row.

        Returns:
            columns by which the table is grouped
        """
        return [x[i] for i in self.keys]

    def __call__(self,
                 rows: TRowsIterable,
                 *args: tp.Any,
                 **kwargs: tp.Any) -> TRowsGenerator:
        """Method for groups two tables by keys and joins them.

        Args:
            rows: first table as an iterator.
            args[0]: second table as an iterator.

        Yields:
            yield table row.
        """

        left_table = groupby(rows, key=self.sort_func)
        right_table = groupby(args[0], key=self.sort_func)
        while True:
            left_keys, left_group_rows = self.my_next(left_table)
            right_keys, right_group_rows = self.my_next(right_table)

            while (left_keys is not None
                   and (right_keys is None or left_keys < right_keys)):
                result_joiner = self.joiner(self.keys,
                                            left_group_rows,
                                            iter({}))
                for row in result_joiner:
                    yield row
                left_keys, left_group_rows = self.my_next(left_table)

            while (right_keys is not None
                   and (left_keys is None or left_keys > right_keys)):
                result_joiner = self.joiner(self.keys,
                                            iter({}),
                                            right_group_rows)
                for row in result_joiner:
                    yield row
                right_keys, right_group_rows = self.my_next(right_table)

            if left_keys is None and right_keys is None:
                break
            elif left_keys == right_keys:
                result_joiner = self.joiner(self.keys,
                                            left_group_rows,
                                            right_group_rows)
                for row in result_joiner:
                    yield row


# Dummy operators


class DummyMapper(Mapper):
    """Yield exactly the row passed"""

    def __call__(self, row: TRow) -> TRowsGenerator:
        """Does not process the table row in any way.

        Args:
            row: one table row.

        Yields:
            yield one table row.
        """
        yield row


class FirstReducer(Reducer):
    """Yield only first row from passed ones"""

    def __call__(self,
                 group_key: tuple[str, ...],
                 rows: TRowsIterable) -> TRowsGenerator:
        """Yields the first row of one table group.

        Args:
            group_key: name columns by which the table is grouped.
            rows: table rows.

        Yields:
            yield table row.
        """
        for row in rows:
            yield row
            break


# Mappers


class MoreTwice(Mapper):
    """Deletes those rows in which the
        value in a certain column is less than 2.

    Attributes:
        column: value column name.
    """

    def __init__(self, column: str):
        """Initializes the instance MoreTwice.

        Args:
            column: value column name.
        """
        self.column = column

    def __call__(self, row: TRow) -> TRowsGenerator:
        """Deletes those lines that have row[self.column] <= 1.

        Args:
            row: one table row.

        Yields:
            yield table row.
        """
        if row[self.column] > 1:
            yield row


class AddDummyColumn(Mapper):
    """Adds a dummy column.

    Attributes:
        column: name of dummy column.
    """

    def __init__(self, column: str):
        """Initializes the instance AddDummyColumn.

        Args:
            column: name of dummy column.
        """
        self.column = column

    def __call__(self, row: TRow) -> TRowsGenerator:
        """Adds a dummy column.

        Args:
            row: one table row.

        Yields:
            yield table row.
        """
        row[self.column] = 1
        yield row


class DeleteDummyColumn(Mapper):
    """Delete a dummy column"""

    def __init__(self, column: str):
        """Initializes the instance DeleteDummyColumn.

        Args:
            column: name of dummy column.
        """
        self.column = column

    def __call__(self, row: TRow) -> TRowsGenerator:
        """Delete a dummy column.

        Args:
            row: one table row.

        Yields:
            yield table row.
        """
        temp_row = {}
        for column in row:
            if column != self.column:
                temp_row[column] = row[column]
        yield temp_row


class LogarithmOfRatioOfColumn(Mapper):
    """Logarithm of the ratio of column.

    Attributes:
        name_frequency_column_in_document: name of the word frequency
            column in the document
        name_frequency_column_in_all_document: name of the word frequency
            column in all documents
    """

    def __init__(self, first_column: str, second_column: str, result_column: str):
        """Initializes the instance LogarithmOfRatioOfColumn.

        Args:
            name_frequency_column_in_document: names of the columns along
                which the join is performed.
            name_frequency_column_in_all_document: table join type.
            result_column:
        """
        self.first_column = first_column
        self.second_column = second_column
        self.result_column = result_column

    def __call__(self, row: TRow) -> TRowsGenerator:
        """Calculates pmi value.

        Args:
            row: one table row.

        Yields:
            yield table row.
        """
        row[self.result_column] = math.log(row[self.first_column] /
                                           row[self.second_column])
        del row[self.first_column]
        del row[self.second_column]
        yield row


class MoreFourCharacters(Mapper):
    """Deletes those rows in which the
        value in a certain column is less than 4.

    Attributes:
        column: value column name.
    """

    def __init__(self, column: str):
        """Initializes the instance MoreFourCharacters.

        Args:
            column: value column name.
        """
        self.column = column

    def __call__(self, row: TRow) -> TRowsGenerator:
        """Deletes those lines that have row[self.column] <= 4.

        Args:
            row: one table row.

        Yields:
            yield table row.
        """
        if len(row[self.column]) > 4:
            yield row


class ConvertToDatetime(Mapper):
    """Converts column values to datetime.

    Attributes:
        columns: value column name.
        format_time: format datetime.
    """

    def __init__(self, columns: list[str], format_time: str):
        """Initializes the instance ConvertToDatetime.

        Args:
            columns: value column name.
            format_time: format datetime.
        """
        self.columns = columns
        self.format_time = format_time

    def __call__(self, row: TRow) -> TRowsGenerator:
        """Converts column values to datetime.

        Args:
            row: one table row.

        Yields:
            yield table row.
        """
        temp_row = {}
        for column in row:
            if column in self.columns:
                try:
                    temp_row[column] = datetime.strptime(row[column],
                                                         self.format_time)
                except ValueError:
                    temp_row[column] = datetime.strptime(row[column],
                                                         self.format_time[:-3])
            else:
                temp_row[column] = row[column]
        print(temp_row)
        yield temp_row


class CompHaversine(Mapper):
    """Calculates Haversine distance for each table row.

    Attributes:
        columns: value column name.
        radius: earth radius.
        result_column: name of result column.
    """

    def __init__(self,
                 columns: tuple[str, str],
                 result_column: str,
                 radius: int):
        """Initializes the instance CompHaversine.

       Args:
            columns: value column name.
            radius: earth radius.
            result_column: name of result column.
       """
        self.columns = columns
        self.radius = radius
        self.result_column = result_column

    @staticmethod
    def haversine(start_coord: list[float], end_coord: list[float], radius: int) -> float:
        """Calculates Haversine distance.

        Args:
            start_coord: coordinates of the first point.
            end_coord: coordinates of the second point.
            radius: earth radius.

        Return:
            returns the distance between points in kilometers.
        """
        lon1, lat1 = start_coord
        lon2, lat2 = end_coord

        d_lat = (lat2 - lat1) * math.pi / 180.0
        d_lon = (lon2 - lon1) * math.pi / 180.0

        lat1 = lat1 * math.pi / 180.0
        lat2 = lat2 * math.pi / 180.0
        a = (pow(math.sin(d_lat / 2), 2) +
             pow(math.sin(d_lon / 2), 2) *
             math.cos(lat1) * math.cos(lat2))
        c = 2 * math.asin(math.sqrt(a))
        return radius * c

    def __call__(self, row: TRow) -> TRowsGenerator:
        """Calculates Haversine distance.

        Args:
            row: one table row.

        Yields:
            yield table row.
        """
        temp_row = {self.result_column: self.haversine(row[self.columns[0]],
                                                       row[self.columns[1]],
                                                       self.radius)}
        for column in row:
            if column not in self.columns:
                temp_row[column] = row[column]
        yield temp_row


class CompTimeDelta(Mapper):
    """Calculates Time Delta.

    Attributes:
        columns: pair of date column names.
        result_column: name of result column.
    """

    def __init__(self, columns: tuple[str, str], result_column: str):
        """Initializes the instance CompTimeDelta.

       Args:
            columns: pair of date column names.
            result_column: name of result column.
       """
        self.columns = columns
        self.result_column = result_column

    def __call__(self, row: TRow) -> TRowsGenerator:
        """Calculates Time Delta.

        Args:
           row: one table row.

        Yields:
           yield table row.
        """

        time_delta = row[self.columns[1]] - row[self.columns[0]]
        row[self.result_column] = time_delta.total_seconds() / 60 / 60
        yield row


class GetWeekdayAndHour(Mapper):
    """Gets the hour and day of the week from a date.

    Attributes:
        time_column: name of time column.
        weekday_result_column: name of weekday result column.
        hour_result_column: name of hour result column.
    """

    def __init__(self,
                 time_column: str,
                 weekday_result_column: str,
                 hour_result_column: str):
        """Initializes the instance GetWeekdayAndHour.

        Args:
            time_column: name of time column.
            weekday_result_column: name of weekday result column.
            hour_result_column: name of hour result column.
        """
        self.time_column = time_column
        self.weekday_result_column = weekday_result_column
        self.hour_result_column = hour_result_column
        self.weekday = {0: "Mon",
                        1: "Tue",
                        2: "Wed",
                        3: "Thu",
                        4: "Fri",
                        5: "Sat",
                        6: "Sun"}

    def __call__(self, row: TRow) -> TRowsGenerator:
        """Gets the hour and day of the week from a date.

        Args:
           row: one table row.

        Yields:
           yield table row.
        """
        hour = row[self.time_column].hour
        weekday = self.weekday[row[self.time_column].weekday()]
        temp_row = {self.hour_result_column: hour,
                    self.weekday_result_column: weekday}
        for column in row:
            if column != self.time_column:
                temp_row[column] = row[column]
        yield temp_row


class FilterPunctuation(Mapper):
    """Left only non-punctuation symbols.

    Attributes:
        column: name of column to process.
    """

    def __init__(self, column: str):
        """Initializes the instance FilterPunctuation.

        Args:
            column: name of column to process.
        """
        self.column = column

    def __call__(self, row: TRow) -> TRowsGenerator:
        """Left only non-punctuation symbols.

        Args:
           row: one table row.

        Yields:
           yield table row.
        """
        filter_punctuation_str = ''
        for symbol in row[self.column]:
            if symbol.isalpha() or symbol.isdigit() or symbol == ' ':
                filter_punctuation_str += symbol
        row[self.column] = filter_punctuation_str
        yield row


class LowerCase(Mapper):
    """Replace column value with value in lower case

    Attributes:
        column: name of column to process.
    """

    def __init__(self, column: str):
        """Initializes the instance LowerCase.

        Args:
            column: name of column to process.
        """
        self.column = column

    @staticmethod
    def _lower_case(txt: str) -> str:
        """Static method converts string to lower case.

        Args:
            txt: string.

        Return:
            returns string lower case
        """
        return txt.lower()

    def __call__(self, row: TRow) -> TRowsGenerator:
        """Left only non-punctuation symbols.

        Args:
           row: one table row.

        Yields:
           yield table row.
        """
        row[self.column] = self._lower_case(row[self.column])
        yield row


class Split(Mapper):
    """Split row on multiple rows by separator.

    Attributes:
        column: name of column to split.
        separator: string to separate by.
    """

    def __init__(self, column: str, separator: str | None = None) -> None:
        """Initializes the instance Split.

        Args:
            column: name of column to split.
            separator: string to separate by.
        """
        self.column = column
        self.separator = separator if separator is not None else "\n\t\u00A0 "

    def split(self, string: str) -> tp.Iterator[str]:
        """Implements the split method as a generator.

        Args:
            string: the string to be split.

        Yields:
            yield string.
        """
        sub_str = ''
        for i in range(len(string)):
            if string[i] not in self.separator:
                sub_str += string[i]
            else:
                yield sub_str
                sub_str = ''
        yield sub_str

    def __call__(self, row: TRow) -> TRowsGenerator:
        """Split row on multiple rows by separator.

        Args:
           row: one table row.

        Yields:
           yield table row.
        """
        split_string = row[self.column]
        for sub_str in self.split(split_string):
            new_row = {}
            for name_column in row:
                if name_column != self.column:
                    new_row[name_column] = row[name_column]
                else:
                    new_row[name_column] = sub_str
            yield new_row


class Product(Mapper):
    """Calculates product of multiple columns.

    Attributes:
        columns: column names to product.
        result_column: column name to save product in.
    """

    def __init__(self,
                 columns: tp.Sequence[str],
                 result_column: str = 'product') -> None:
        """Initializes the instance Product.

        Args:
            columns: column names to product.
            result_column: column name to save product in.
        """
        self.columns = columns
        self.result_column = result_column

    def __call__(self, row: TRow) -> TRowsGenerator:
        """Calculates product of multiple columns.

        Args:
           row: one table row.

        Yields:
           yield table row.
        """
        prod = 1
        for column in self.columns:
            prod *= row[column]
        row[self.result_column] = prod
        yield row


class Filter(Mapper):
    """Remove records that don't satisfy some condition.

    Attributes:
        condition: if condition is not true - remove record
    """

    def __init__(self, condition: tp.Callable[[TRow], bool]) -> None:
        """Initializes the instance Filter.

        Args:
            condition: if condition is not true - remove record
        """
        self.condition = condition

    def __call__(self, row: TRow) -> TRowsGenerator:
        """Remove records that don't satisfy some condition.

        Args:
           row: one table row.

        Yields:
           yield table row.
        """
        if self.condition(row):
            yield row


class Project(Mapper):
    """Leave only mentioned columns.

    Attributes:
        columns: names of columns.

    """

    def __init__(self, columns: tp.Sequence[str]) -> None:
        """Initializes the instance Filter.

        Args:
            columns: names of columns
        """
        self.columns = columns

    def __call__(self, row: TRow) -> TRowsGenerator:
        """Leave only mentioned columns.

        Args:
           row: one table row.

        Yields:
           yield table row.
        """
        result = {name_columns: row[name_columns]
                  for name_columns in self.columns}
        yield result


class AverageSpeed(Mapper):
    """Calculates average speed.

    Attributes:
        columns: a pair of column names with time and distance.
        result_column: name result column.
    """

    def __init__(self, columns: tuple[str, str], result_column: str):
        """Initializes the instance AverageSpeed.

        Args:
            columns: a pair of column names with time and distance.
            result_column: name result column.
        """
        self.columns = columns
        self.result_column = result_column

    def __call__(self, row: TRow) -> TRowsGenerator:
        """Calculates average speed.

        Args:
           row: one table row.

        Yields:
           yield table row.
        """
        average_speed = row[self.columns[0]] / row[self.columns[1]]
        temp_row = {self.result_column: average_speed}
        for columns in row:
            if columns not in self.columns:
                temp_row[columns] = row[columns]
        yield temp_row


# Reducers


class TopN(Reducer):
    """Calculate top N by value.

    Attributes:
        column: column name to get top by.
        n: number of top values to extract.
    """

    def __init__(self, column: str, n: int) -> None:
        """Initializes the instance TopN.

        Args:
            columns: a pair of column names with time and distance.
            result_column: name result column.
        """
        self.column_max = column
        self.n = n

    def __call__(self,
                 group_key: tuple[str, ...],
                 rows: TRowsIterable) -> TRowsGenerator:
        """Calculate top N by value.

        Args:
            group_key: name columns by which the table is grouped.
            rows: table rows.

        Yields:
            yield table rows.
        """
        h: list[tuple[tp.Any, int, TRow]] = []
        for num_row, row in enumerate(rows):
            heapq.heappush(h, (row[self.column_max], num_row, row))
            if len(h) == self.n + 1:
                heapq.heappop(h)
        for i, num_row, row in h[::-1]:
            yield row


class TermFrequency(Reducer):
    """Calculate frequency of values in column.

    Attributes:
        words_column: name for column with words.
        result_column: name for result column.
    """

    def __init__(self, words_column: str, result_column: str = 'tf') -> None:
        """Initializes the instance TermFrequency.

        Args:
            words_column: name for column with words.
            result_column: name for result column.
        """
        self.words_column = words_column
        self.result_column = result_column

    def __call__(self,
                 group_key: tuple[str, ...],
                 rows: TRowsIterable) -> TRowsGenerator:
        """Calculate frequency of values in column.

        Args:
            group_key: name columns by which the table is grouped.
            rows: table rows.

        Yields:
            yield table rows.
        """
        counter: dict[str, int] = Counter()
        count_row = 0
        list_rows = []
        for row in rows:
            if row[self.words_column] not in counter:
                list_rows.append(row)
            counter[row[self.words_column]] += 1
            count_row += 1
        for row in list_rows:
            row[self.result_column] = (counter[row[self.words_column]]
                                       / count_row)
            if "count" in row:
                del row['count']
            yield row


class Count(Reducer):
    """
    Count records by key.
    Example for group_key=('a',) and column='d'
        {'a': 1, 'b': 5, 'c': 2}
        {'a': 1, 'b': 6, 'c': 1}
        =>
        {'a': 1, 'd': 2}


    Attributes:
        column: name for result column.
    """

    def __init__(self, column: str) -> None:
        """Initializes the instance TermFrequency.

        Args:
            column: name for result column.
        """
        self.column = column

    def __call__(self,
                 group_key: tuple[str, ...],
                 rows: TRowsIterable) -> TRowsGenerator:
        """Count records by key.

        Args:
            group_key: name columns by which the table is grouped.
            rows: table rows.

        Yields:
            yield table rows.
        """
        if group_key[0] == self.column:
            new_row: dict[str, tp.Any] = {self.column: 0}
            for row in rows:
                new_row[self.column] += 1
            yield new_row

        else:
            temp_row = {}
            for row in rows:
                temp_row = {column: row[column] for column in group_key}
                temp_row[self.column] = 1
                break
            for row in rows:
                temp_row[self.column] += 1
            yield temp_row


class Sum(Reducer):
    """
    Sum values aggregated by key.
    Example for key=('a',) and column='b'
        {'a': 1, 'b': 2, 'c': 4}
        {'a': 1, 'b': 3, 'c': 5}
        =>
        {'a': 1, 'b': 5}

    Attributes:
        column: name for sum column.
    """

    def __init__(self, column: str) -> None:
        """Initializes the instance Sum.

        Args:
            column: name for sum column.
        """
        self.column = column

    def __call__(self,
                 group_key: tuple[str, ...],
                 rows: TRowsIterable) -> TRowsGenerator:
        """Sum values aggregated by key.

        Args:
            group_key: name columns by which the table is grouped.
            rows: table rows.

        Yields:
            yield table rows.
        """
        new_row = {self.column: 0, group_key[0]: ""}
        for row in rows:
            new_row[group_key[0]] = row[group_key[0]]
            new_row[self.column] += row[self.column]
        yield new_row


# Joiners


class InnerJoiner(Joiner):
    """Join with inner strategy."""

    def __call__(self,
                 keys: tp.Sequence[str],
                 rows_a: TRowsIterable,
                 rows_b: TRowsIterable) -> TRowsGenerator:
        """Join with inner strategy.

        Args:
            keys: names of the columns along which the join is performed.
            rows_a: first table as an iterator.
            rows_b: second table as an iterator.

        Yields:
            yield table rows.
        """
        row_a, row_b = self.is_empty(rows_a, rows_b)
        if row_a is not None and row_b is not None:
            mearge_row = self.mearge_table(keys, row_a, row_b, rows_a, rows_b)
            for row in mearge_row:
                yield row


class OuterJoiner(Joiner):
    """Join with outer strategy."""

    def __call__(self,
                 keys: tp.Sequence[str],
                 rows_a: TRowsIterable,
                 rows_b: TRowsIterable) -> TRowsGenerator:
        """Join with outer strategy.

        Args:
            keys: names of the columns along which the join is performed.
            rows_a: first table as an iterator.
            rows_b: second table as an iterator.

        Yields:
            yield table rows.
        """
        row_a, row_b = self.is_empty(rows_a, rows_b)
        if row_b is None and row_a is not None:
            yield row_a
            for row in rows_a:
                yield row
        elif row_a is None and row_b is not None:
            yield row_b
            for row in rows_b:
                yield row
        elif row_a is not None and row_b is not None:
            mearge_row = self.mearge_table(keys, row_a, row_b, rows_a, rows_b)
            for row in mearge_row:
                yield row


class LeftJoiner(Joiner):
    """Join with left strategy."""

    def __call__(self,
                 keys: tp.Sequence[str],
                 rows_a: TRowsIterable,
                 rows_b: TRowsIterable) -> TRowsGenerator:
        """Join with left strategy.

        Args:
            keys: names of the columns along which the join is performed.
            rows_a: first table as an iterator.
            rows_b: second table as an iterator.

        Yields:
            yield table rows.
        """
        row_a, row_b = self.is_empty(rows_a, rows_b)
        if row_b is None and row_a is not None:
            yield row_a
            for row in rows_a:
                yield row
        elif row_a is not None and row_b is not None:
            mearge_row = self.mearge_table(keys, row_a, row_b, rows_a, rows_b)
            for row in mearge_row:
                yield row


class RightJoiner(Joiner):
    """Join with right strategy."""

    def __call__(self,
                 keys: tp.Sequence[str],
                 rows_a: TRowsIterable,
                 rows_b: TRowsIterable) -> TRowsGenerator:
        """Join with right strategy.

        Args:
            keys: names of the columns along which the join is performed.
            rows_a: first table as an iterator.
            rows_b: second table as an iterator.

        Yields:
            yield table rows.
        """
        row_a, row_b = self.is_empty(rows_a, rows_b)
        if row_a is None and row_b is not None:
            yield row_b
            for row in rows_b:
                yield row
        elif row_a is not None and row_b is not None:
            mearge_row = self.mearge_table(keys, row_a, row_b, rows_a, rows_b)
            for row in mearge_row:
                yield row


def parser(line: str) -> TRow:
    """Preprocessing strings after reading from file.

    Args:
        line: file line.

    Return:
        returns a dictionary.
    """
    return dict(eval(line))
