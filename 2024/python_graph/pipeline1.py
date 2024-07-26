def read_data(*args, **kwargs) -> None:
	return 1


def process(data: int) -> int:
	return data + 1


def write_data(data: int) -> None:
	print(data)


def pipeline():
	raw_data = read_data()
	processed_data = process(raw_data)
	write_data(processed_data)
