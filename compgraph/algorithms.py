import typing as tp
import compgraph.operations as ops
from . import Graph, operations


def word_count_graph(
        input_stream_name: str,
        text_column: str = 'text',
        count_column: str = 'count',
        parser: tp.Optional[tp.Callable[[str], ops.TRow]] = None
) -> Graph:
    """Constructs graph which counts words in text_column of all rows passed.

    Args:
        input_stream_name: name of the iterator to read the table.
        text_column: text column name.
        count_column: result column name.
        parser: preprocessing strings when reading them from a file.

    Return:
        returns graph which counts words in text_column of all rows passed.
    """

    return Graph.graph_from_(input_stream_name, parser) \
        .map(operations.FilterPunctuation(text_column)) \
        .map(operations.LowerCase(text_column)) \
        .map(operations.Split(text_column)) \
        .sort([text_column]) \
        .reduce(operations.Count(count_column), [text_column]) \
        .sort([count_column, text_column])


def inverted_index_graph(
        input_stream_name: str,
        doc_column: str = 'doc_id',
        text_column: str = 'text',
        result_column: str = 'tf_idf',
        parser: tp.Optional[tp.Callable[[str], ops.TRow]] = None
) -> Graph:
    """Constructs graph which calculates td-idf for every word/document pair.

    Args:
        input_stream_name: name of the iterator to read the table.
        doc_column: column name with document id.
        text_column: text column name.
        result_column: result column name.
        parser: preprocessing strings when reading them from a file.

    Return:
        returns graph which calculates td-idf for every word/document pair.
    """
    сount_doc = Graph.graph_from_(input_stream_name, parser) \
        .reduce(operations.Count("сount_doc"), [doc_column]) \
        .reduce(operations.Count("сount_doc"), ["сount_doc"]) \
        .map(operations.AddDummyColumn("dummy_column"))

    split_word_1 = Graph.graph_from_(input_stream_name, parser) \
        .map(operations.LowerCase(text_column)) \
        .map(operations.FilterPunctuation(text_column)) \
        .map(operations.Split(text_column))

    idf = split_word_1 \
        .sort([doc_column, text_column]) \
        .reduce(operations.FirstReducer(), [doc_column, text_column]) \
        .sort([text_column]) \
        .reduce(operations.Count("count"), [text_column]) \
        .map(operations.AddDummyColumn("dummy_column")) \
        .join(operations.InnerJoiner(), сount_doc, ["dummy_column"]) \
        .map(operations.DeleteDummyColumn("dummy_column")) \
        .map(operations.LogarithmOfRatioOfColumn("сount_doc", "count", "idf"))

    split_word_2 = Graph.graph_from_(input_stream_name, parser) \
        .map(operations.LowerCase(text_column)) \
        .map(operations.FilterPunctuation(text_column)) \
        .map(operations.Split(text_column))

    tf = split_word_2 \
        .sort([doc_column]) \
        .reduce(operations.TermFrequency(text_column), [doc_column]) \
        .sort([text_column])

    return tf \
        .join(operations.InnerJoiner(), idf, [text_column]) \
        .map(operations.Product(['tf', 'idf'], result_column)) \
        .map(operations.DeleteDummyColumn("tf")) \
        .map(operations.DeleteDummyColumn("idf")) \
        .sort([text_column]) \
        .reduce(operations.TopN(result_column, 3), [text_column])


def pmi_graph(
        input_stream_name: str,
        doc_column: str = 'doc_id',
        text_column: str = 'text',
        result_column: str = 'pmi',
        parser: tp.Optional[tp.Callable[[str], ops.TRow]] = None
) -> Graph:
    """Constructs graph which gives for every document
        the top 10 words ranked by pointwise mutual information.

    Args:
        input_stream_name: name of the iterator to read the table.
        doc_column: column name with document id.
        text_column: text column name.
        result_column: result column name.
        parser: preprocessing strings when reading them from a file.

    Return:
        returns graph which gives for every document
            the top 10 words ranked by pointwise mutual information
    """
    split_word_1 = Graph.graph_from_(input_stream_name, parser) \
        .map(operations.LowerCase(text_column)) \
        .map(operations.FilterPunctuation(text_column)) \
        .map(operations.Split(text_column))

    true_word_1 = split_word_1 \
        .sort([doc_column, text_column]) \
        .reduce(operations.Count('count'), [doc_column, text_column]) \
        .map(operations.MoreTwice("count")) \
        .sort([doc_column, text_column])

    split_word_2 = Graph.graph_from_(input_stream_name, parser) \
        .map(operations.LowerCase(text_column)) \
        .map(operations.FilterPunctuation(text_column)) \
        .map(operations.Split(text_column)) \
        .sort([doc_column, text_column])

    first = split_word_2 \
        .join(operations.InnerJoiner(),
              true_word_1, [doc_column, text_column]) \
        .sort([doc_column]) \
        .reduce(operations.TermFrequency(text_column, "first"), [doc_column]) \
        .sort([text_column])

    split_word_3 = Graph.graph_from_(input_stream_name, parser) \
        .map(operations.LowerCase(text_column)) \
        .map(operations.FilterPunctuation(text_column)) \
        .map(operations.Split(text_column)) \
        .sort([doc_column, text_column])

    true_word_2 = split_word_3 \
        .sort([doc_column, text_column]) \
        .reduce(operations.Count('count'), [doc_column, text_column]) \
        .map(operations.MoreTwice("count")) \
        .sort([doc_column, text_column])

    split_word_4 = Graph.graph_from_(input_stream_name, parser) \
        .map(operations.LowerCase(text_column)) \
        .map(operations.FilterPunctuation(text_column)) \
        .map(operations.Split(text_column)) \
        .sort([doc_column, text_column])

    second = split_word_4 \
        .join(operations.InnerJoiner(),
              true_word_2, [doc_column, text_column]) \
        .sort([text_column]) \
        .map(operations.AddDummyColumn("dummy")) \
        .reduce(operations.TermFrequency(text_column, "second"), ["dummy"]) \
        .map(operations.DeleteDummyColumn('doc_id')) \
        .map(operations.DeleteDummyColumn('dummy')) \
        .sort([text_column])

    return first \
        .join(operations.InnerJoiner(), second, [text_column]) \
        .map(operations.LogarithmOfRatioOfColumn("first", "second", "pmi")) \
        .sort([doc_column]) \
        .reduce(operations.TopN(result_column, 10), [doc_column])


def yandex_maps_graph(
        input_stream_name_time: str,
        input_stream_name_length: str,
        enter_time_column: str = 'enter_time',
        leave_time_column: str = 'leave_time',
        edge_id_column: str = 'edge_id',
        start_coord_column: str = 'start',
        end_coord_column: str = 'end',
        weekday_result_column: str = 'weekday',
        hour_result_column: str = 'hour',
        speed_result_column: str = 'speed',
        parser: tp.Optional[tp.Callable[[str], ops.TRow]] = None
) -> Graph:
    """Constructs graph which measures average
        speed in km/h depending on the weekday and hour.

    Args:
        input_stream_name_time: name of the iterator to read the table time.
        input_stream_name_length: name of the
            iterator to read the table length.
        enter_time_column: name of the column with road entry data.
        leave_time_column: name of the column with road leave data.
        edge_id_column: column name with road id.
        start_coord_column: name of the column
            with the starting coordinate of the road.
        end_coord_column: name of the column with
            the final coordinate of the road.
        weekday_result_column: result column name for weekday.
        hour_result_column: result column name for hour.
        speed_result_column: result column name for speed.
        parser: preprocessing strings when reading them from a file.

    Return:
        returns graph which measures average
            speed in km/h depending on the weekday and hour.
    """
    length_edge_1 = Graph.graph_from_(input_stream_name_length, parser) \
        .map(operations.CompHaversine((start_coord_column,
                                       end_coord_column),
                                      "length",
                                      6373)) \
        .sort([edge_id_column])

    time_delta_and_length_1 = Graph.graph_from_(
        input_stream_name_time,
        parser) \
        .map(
        operations.ConvertToDatetime(
            [enter_time_column, leave_time_column],
            '%Y%m%dT%H%M%S.%f')
    ) \
        .map(
        operations.CompTimeDelta((enter_time_column,
                                  leave_time_column),
                                 'time_delta')) \
        .sort([edge_id_column]) \
        .join(operations.InnerJoiner(), length_edge_1, [edge_id_column]) \
        .map(operations.DeleteDummyColumn(leave_time_column))

    sum_time_delta = time_delta_and_length_1 \
        .sort([enter_time_column]) \
        .reduce(operations.Sum("time_delta"), [enter_time_column]) \
        .sort([enter_time_column])

    length_edge_2 = Graph.graph_from_(input_stream_name_length, parser) \
        .map(
        operations.CompHaversine((start_coord_column,
                                  end_coord_column),
                                 "length",
                                 6373)) \
        .sort([edge_id_column])

    time_delta_and_length_2 = Graph.graph_from_(
        input_stream_name_time,
        parser) \
        .map(
        operations.ConvertToDatetime(
            [enter_time_column, leave_time_column],
            '%Y%m%dT%H%M%S.%f')) \
        .map(
        operations.CompTimeDelta((enter_time_column,
                                  leave_time_column),
                                 'time_delta')) \
        .sort([edge_id_column]) \
        .join(operations.InnerJoiner(), length_edge_2, [edge_id_column]) \
        .map(operations.DeleteDummyColumn(leave_time_column))

    sum_length = time_delta_and_length_2 \
        .sort([enter_time_column]) \
        .reduce(operations.Sum("length"), [enter_time_column]) \
        .sort([enter_time_column])

    return sum_length \
        .join(operations.InnerJoiner(), sum_time_delta, [enter_time_column]) \
        .map(operations.AverageSpeed(('length', 'time_delta'),
                                     speed_result_column)) \
        .map(operations.GetWeekdayAndHour(enter_time_column,
                                          weekday_result_column,
                                          hour_result_column))
