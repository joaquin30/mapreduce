MapReduce
==========

Requirements
-------------

- Python 3.6+ (for subprocess.run)
- C++11 compiler (for reduce script)


Use
----

    python mapreduce.py [map OR reduce] [path_to_script] [source_file] [destination_file]


WordCount example (in Linux)
-----------------------------

- Compile reduce.cpp
    g++ -o reduce reduce.cpp
- Run map script
    python mapreduce.py map 'python map.py' example.txt output1.txt
- Run reduce script
    python mapreduce.py reduce './reduce' output1.txt output2.txt


Notes
------

map operation sends a TSV from the source file to the map script of the form:

    [number_of_line]\t[line]\n

