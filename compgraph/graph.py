import typing as tp

from . import operations as ops
from . import external_sort as ex


class Graph:
    """Computational graph implementation.

    Attributes:
        reader: way to read data.
        operations: list of graph operations.
    """

    def __init__(self, reader: ops.Operation):
        """Initializes the instance Graph.

        Args:
            reader: way to read data.
            operations: list of graph operations.
        """
        self.reader = reader
        self.operations: list[tuple[ops.Operation, tp.Any]] = []

    def push_operations(self, operation: ops.Operation, *args: tp.Any) -> None:
        """Adding a new operation to the graph.

        Args:
            operation: new operation.
        """
        self.operations.append((operation, args))

    @staticmethod
    def graph_from_iter(name: str) -> 'Graph':
        """Construct new graph which reads data from
            row iterator (in form of sequence of Rows
        from 'kwargs' passed to 'run' method) into graph data-flow.
        Use ops.ReadIterFactory.

        Args:
            name: name of kwarg to use as data source.

        Return:
            return graph.
        """
        return Graph(ops.ReadIterFactory(name))

    @staticmethod
    def graph_from_file(name: str,
                        parser: tp.Callable[[str], ops.TRow]) -> 'Graph':
        """Construct new graph extended with
            operation for reading rows from file.
        Use ops.Read

        Args:
            name: name of kwarg to use as data source.
            parser: parser from string to Row.

        Return:
            return graph.
        """
        return Graph(ops.Read(name, parser))

    @staticmethod
    def graph_from_(
            name: str,
            parser: tp.Optional[tp.Callable[[str], ops.TRow]] = None
    ) -> 'Graph':
        """Construct new graph extended with
            operation for reading rows from file or row iterator.
        Use ops.Read

        Args:
            name: name of kwarg to use as data source.
            parser: parser from string to Row.

        Return:
            return graph.
        """
        if parser is None:
            return Graph(ops.ReadIterFactory(name))
        else:
            return Graph(ops.Read(name, parser))

    def map(self, mapper: ops.Mapper) -> 'Graph':
        """Construct new graph extended with
            map operation with particular mapper.

        Args:
            mapper: mapper to use.

        Return:
            return graph.
        """
        self.push_operations(ops.Map(mapper))
        return self

    def reduce(self, reducer: ops.Reducer, keys: tp.Sequence[str]) -> 'Graph':
        """Construct new graph extended with
            reduce operation with particular reducer.

        Args:
            reducer: reducer to use.
            keys: keys for grouping.

        Return:
            return graph.
        """
        self.push_operations(ops.Reduce(reducer, keys))
        return self

    def sort(self, keys: tp.Sequence[str]) -> 'Graph':
        """Construct new graph extended with sort operation.

        Args:
            keys: sorting keys (typical is tuple of strings).
        Return:
            return graph.
        """
        self.push_operations(ex.ExternalSort(keys))
        return self

    def join(self,
             joiner: ops.Joiner,
             join_graph: 'Graph',
             keys: tp.Sequence[str]) -> 'Graph':
        """Construct new graph extended with join operation with another graph.

        Args:
            joiner: join strategy to use.
            join_graph: other graph to join with.
            keys: keys for grouping.

        Return:
            return graph.
        """
        self.push_operations(ops.Join(joiner, keys), join_graph)
        return self

    def run(self, **kwargs: tp.Any) -> ops.TRowsIterable:
        """Single method to start execution; data sources passed as kwargs.

        Return:
            return result graph values.
        """
        read = self.reader(**kwargs)
        for operation, args in self.operations:
            if isinstance(operation, ops.Join):
                read = operation(read, args[0].run(**kwargs))
            else:
                read = operation(read)
        for row in read:
            yield row
