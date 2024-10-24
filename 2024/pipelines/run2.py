import inspect

import ast


# this removes the need of a transformation and just uses functions and passing output from one to another based on static args/kwargs

class Operation:
    def __init__(self):
        self.downstream = list()

    def __rshift__(self, other):
        self.downstream.append(other)
        return self


class DAG:
    def run(self):
        output = None
        for function in self.downstream:
            entire_ast = ast.parse(inspect.getsource(function))
            func_args = entire_ast.body[0].args.args
            if func_args:
                output = function(output)
            else:
                output = function()

        return output


def fetch_data_api():
    print("FETCH DONE")
    return [1, 2, 3]


def filter_fields(inp):
    print("FILTER DONE")
    return [e for e in inp if e > 1]


def dump_to_file(inp):
    print(inp)
    print("DUMP DONE")


dag = DAG() >> fetch_data_api >> filter_fields >> dump_to_file
print(dag)
print(dag.downstream)
print(dag.run())

# TODO: extend kwargs etc. in run
# TODO: can we handle args & kwargs too within run
# TODO: implement load modules to find dag objects in globals()
# TODO: implement rrshift etc. for [a, b] >> c; how to pass output between them?
# TODO: static args/kwargs?