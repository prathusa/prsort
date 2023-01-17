psort: psort.c
	gcc psort.c -o psort -g3 -O3 -Wall -Werror -pthread


test: psort
	./psort input.raw output.raw	

1gb: gen_tests.py
	python gen_tests.py 10000000 input.raw expected.raw # windows
	python3 gen_tests.py 10000000 input.raw expected.raw # linux/macOS
