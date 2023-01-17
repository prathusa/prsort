# Parallelized Radix Sort - prsort

## Usage

### Compile
`make`

### Run Sort (default input is 10 Mb)
`make test`


### Generate 1 Gb test
`make 1gb` 

Check the makefile and run the line for your specific platform

### Generate your own N * 10 bytes size test

Windows: `python gen_test.py N input.raw expected.raw`

Linux/MacOS: `python3 gen_test.py N input.raw expected.raw`

You can use the default test command `make test` to test after file generation.

### Verification - Compare output from expected
`diff output.raw expected.raw`
