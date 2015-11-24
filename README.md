# Hadoop_map_reduce_Equijoin

Implemented a hadoop map-reduce program with 4 slaves for Equijoin Operation

Tips:
1. Create jar file of the .java file.

Command to execute:
sudo -u <username> <path_of_hadoop> jar <name_of_jar> <class_with_main_function> <HDFSinputFile> <HDFSoutputFile>

Mapper Class:

1. Trim the line (remove all extra spaces)
2. Take the JoinColumn of the line and pass the value which refers to that key to the reducer

Reducer:

1. Make 2 Lists, one for each table, and for each key sperate the values into appropriate lists.
2. Check whether there's only 1 record in R list or just 1 in S list. This means it cant be joined. If not , goto step 3.
3. For each value in "R" List, print the corresponding "S" value to the context.

Driver:

1. Set the mapper and reducer class.
2. Set the appropriate output key and value class types
3. Set the jar by class name.
4. Set the input and output format classes.
5. Give the I/P and O/P path as args.
6. Wait for job completition
