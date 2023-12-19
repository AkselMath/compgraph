import click

from compgraph.algorithms import yandex_maps_graph
import compgraph.operations as ops


@click.command()
@click.option('--filepath_travel_time',
              help='file path travel time.')
@click.option('--filepath_edge_length',
              help='file path edge length.')
@click.option('--output_filepath',
              default="result_filepath",
              help='output file path.')
def main(
        filepath_travel_time: str,
        filepath_edge_length: str,
        output_filepath: str
) -> None:
    """Call the yandex_maps_graph function via the command line.

    Arg:
        filepath_travel_time: path to the file with movement data.
        filepath_edge_length: path to the file with distance data.
        output_filepath: result file path.
    """
    graph = yandex_maps_graph(
        input_stream_name_time='travel_time',
        input_stream_name_length='edge_length',
        enter_time_column='enter_time',
        leave_time_column='leave_time',
        edge_id_column='edge_id',
        start_coord_column='start',
        end_coord_column='end',
        weekday_result_column='weekday',
        hour_result_column='hour',
        speed_result_column='speed',
        parser=ops.parser
    )

    result = graph.run(
        travel_time=lambda: filepath_travel_time,
        edge_length=lambda: filepath_edge_length
    )

    with open(output_filepath, "w") as out:
        for row in result:
            print(row, file=out)


if __name__ == "__main__":
    main()
