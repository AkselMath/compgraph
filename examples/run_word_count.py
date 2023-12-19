import click

from compgraph.algorithms import word_count_graph
import compgraph.operations as ops


@click.command()
@click.option('--input_filepath',
              help='input file path.')
@click.option('--output_filepath',
              default="result_filepath",
              help='output file path.')
def main(input_filepath: str, output_filepath: str) -> None:
    """Call the word_count_graph function via the command line.

    Arg:
        input_filepath: path to the data file.
        output_filepath: result file path.
    """
    graph = word_count_graph(
        input_stream_name="input",
        text_column='text',
        count_column='count',
        parser=ops.parser
    )

    result = graph.run(input=lambda: input_filepath)
    with open(output_filepath, "w") as out:
        for row in result:
            print(row)
            print(row, file=out)


if __name__ == "__main__":
    main()
