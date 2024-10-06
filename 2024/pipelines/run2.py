# tl;dr how to structure the code ~ airflow + dagster but without any dependencies
from typing import Any, Optional

from abc import abstractmethod

import abc


# this removes the need of a transformation and just uses functions

class GraphElement:
    ...


class Node(GraphElement):
    ...


# TODO: will this work dynamically?
class Edge(GraphElement):
    def __init__(self):
        self.downstream = list()

    def __rshift__(self, other):
        self.downstream.append(other)
        return self


class DAG(Edge):
    def run(self):
        output = None
        for function in self.downstream:
            output = function(output)

        return output


def fetch_data_api(previous_output):
    print("FETCH DONE")


def filter_fields(previous_output):
    print("FILTER DONE")


def dump_to_file(previous_output):
    print("DUMP DONE")


dag = DAG() >> fetch_data_api >> filter_fields >> dump_to_file
print(dag)
print(dag.downstream)
print(dag.run())

# TODO: need a dir to fetch all dags from via a function
