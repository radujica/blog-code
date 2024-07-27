def read_data(*args, **kwargs) -> None:
	return 1


def process(data: int) -> int:
	return data + 1


def write_data(data: int) -> None:
	print(data)


def pipeline(not_a_node_arg: int):
	raw_data = read_data(not_a_node_arg, not_a_node_kwarg=42)
	not_a_node_variable = not_a_node_arg
	processed_data = process(data=raw_data)
	write_data(processed_data)
	print(not_a_node_arg)
