#!/bin/sh

cat __output_sequential.txt | sort > __output_sequential_sort.txt;cat __output_parallel.txt | sort > __output_parallel_sort.txt
diff __output_parallel_sort.txt __output_sequential_sort.txt
