import click

from compgraph.algorithms import inverted_index_graph
import compgraph.operations as ops


@click.command()
@click.option('--input_filepath',
              help='input file path.')
@click.option('--output_filepath',
              default="result_filepath",
              help='output file path.')
def main(input_filepath: str, output_filepath: str) -> None:
    """Call the inverted_index_graph function via the command line.

    Arg:
        input_filepath: path to the data file.
        output_filepath: result file path.
    """
    graph = inverted_index_graph(
        input_stream_name="input",
        doc_column='doc_id',
        text_column='text',
        result_column='tf_idf',
        parser=ops.parser
    )

    result = graph.run(input=lambda: input_filepath)
    with open(output_filepath, "w") as out:
        for row in result:
            print(row, file=out)


if __name__ == "__main__":
    main()
