# tl;dr how to structure the code ~ airflow + dagster but without any dependencies
from typing import Any, Optional

from abc import abstractmethod

import abc


class GraphElement:
    ...


class Node(GraphElement):
    ...


# TODO: will this work dynamically?
class Edge(GraphElement):
    def __init__(self):
        self.upstream = list()
        self.downstream = list()

    def __rshift__(self, other):
        self.downstream.append(other)
        return self

    # TODO: not critical here
    def __lshift__(self, other):
        ...

    # TODO: these when doing lists
    def __rrshift__(self, other):
        ...

    def __rlshift__(self, other):
        ...


# # could have flag to materialize
# class DataInput(Node, abc.ABC):
#     @abstractmethod
#     def read(self):
#         raise NotImplementedError
#
#
# class DataOutput(Node, abc.ABC):
#     @abstractmethod
#     def write(self):
#         raise NotImplementedError
#
#
# class Api(DataInput):
#     def read(self):
#         ...
#
#
# class File(DataOutput):
#     def write(self):
#         ...
#
#
# # TODO: challenge, how to make this more automatic
# class FilteredData(DataInput, DataOutput):
#     def read(self):
#         pass
#
#     def write(self):
#         pass


class Transformation(Edge, abc.ABC):
    @abstractmethod
    def run(self, previous_output: Optional[Any]):
        raise NotImplementedError


class FetchDataApi(Transformation):
    def run(self, previous_output: Optional[Any]):
        print(f"{self.__class__.__name__} DONE")


class FilterFields(Transformation):
    def run(self, previous_output: Optional[Any]):
        print(f"{self.__class__.__name__} DONE")


class DumpToFile(Transformation):
    def run(self, previous_output: Optional[Any]):
        print(f"{self.__class__.__name__} DONE")


def run_all(first_node: Transformation) -> Optional[Any]:
    output = first_node.run(None)
    for transformation in first_node.downstream:
        output = transformation.run(output)

    return output


dag = FetchDataApi() >> FilterFields() >> DumpToFile()
print(dag)
print(dag.downstream)

run_all(dag)

# TODO: need a dir to fetch all dags from via a function
