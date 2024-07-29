import ast
import inspect
from typing import List

def extract_input(elem, nodes) -> List[str]:
	args = list()
	for arg in elem.value.args:
		if isinstance(arg, ast.Name):
			args.append(arg.id)
		elif isinstance(arg, ast.Constant):
			args.append(arg.value)
	kwargs = list()
	for kwarg in elem.value.keywords:
		if isinstance(kwarg, ast.Name):
			kwargs.append(kwarg.value.id)
		elif isinstance(kwarg, ast.keyword) and isinstance(kwarg.value, ast.Name):
			kwargs.append(kwarg.value.id)
	inputs = [input_ for input_ in args + kwargs if input_ in nodes]
	
	return inputs

def infer_graph(func):
	entire_ast = ast.parse(inspect.getsource(func))
	entire_pipeline_func = entire_ast.body[0]
	edges = list()
	nodes = set()
	for elem in entire_pipeline_func.body:
		if isinstance(elem, ast.Assign) and isinstance(elem.value, ast.Call):
			# e.g. given: processed_data = process(data)
			output_ = elem.targets[0].id  # processed_data
			nodes.add(output_)
			f = elem.value.func.id  # process
			input_ = extract_input(elem, nodes)  # [data]

			edges.append((
				input_, f, output_
			))
		elif isinstance(elem, ast.Expr):
			f = elem.value.func.id
			input_ = extract_input(elem, nodes)
			if input_:
				edges.append((
					input_, f, None
				))

	return edges
