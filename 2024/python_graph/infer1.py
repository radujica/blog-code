import ast
import inspect

def infer_graph(func):
	entire_ast = ast.parse(inspect.getsource(func))
	entire_pipeline_func = entire_ast.body[0]
	edges = list()
	for elem in entire_pipeline_func.body:
		if isinstance(elem, ast.Assign):
			# e.g. given: processed_data = process(data)
			output_ = elem.targets[0].id  # processed_data
			f = elem.value.func.id  # process
			input_ = elem.value.args[0].id if elem.value.args else None  # data

			edges.append((
				input_, f, output_
			))
		elif isinstance(elem, ast.Expr):
			f = elem.value.func.id
			input_ = elem.value.args[0].id if elem.value.args else None 
			edges.append((
				input_, f, None
			))

	return edges
