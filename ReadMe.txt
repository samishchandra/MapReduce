Following are the commands used for running the user programs

1.	python -m distalgo.runtime main_grep.da
2.	python -m distalgo.runtime main_wordcount.da

Following are the commands used for running the user programs with command line arguments

<first parameter> – asd fsdf input file name
<second parameter> – input split size
<third parameter> – no of map workers
<fourth parameter> – no of reduce workers

1.	python -m distalgo.runtime main_grep.da inputFile.txt 3 2 2
2.	python -m distalgo.runtime main_wordcount.da inputFile.txt 3 2 2

Various parameters of the API can be set via user programs. This can be seen in main function of both the files. Please make sure the directory structure remains the same.

Since Fault Tolerance is a very special case to test, I have made a variable testFaultTolerance in MapReduce.da file. This is found at line number 18 in MapReduce.da file. If the variable is set to true then we can test the fault tolerance mechanism. The file randomly choose the workers and make them not to respond to pings. Hence sometimes all the workers can be dead and the program doesn’t terminate. The situation where no worker is available, then the boot strapper program has to restart the execution which beyond the scope of this implementation.
