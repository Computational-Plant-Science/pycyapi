test:
	python3 -m pytest

lint:
	python3 -m pylint --enable=spelling --spelling-dict en_US --spelling-private-dict-file tests/spelling_dict.txt  clusterside
