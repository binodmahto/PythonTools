# Python File Splitter
It's a file splitter tool which chunks the large csv files into smaller csv files so that it can be processed efficiently in memory by any programming language or techniques. 

#### Example Scenario
There are huge data files in csv format which needs to be processed and dump data in database. Limitation with any data migration tool/technique (i.e. bulk upload, SSIS etc) is 
***Load and Processing huge files in memory can result OutOfMemory Exception***. This tools efficiently helps to split file(s) into smaller chunks which can be then easily 
processed. This tool also has capability to process and chunk Parent and child data files (i.e. consider a scenario of Parent and foregin key relationship data) as well where Parent
file has the Parent data and the child file has the child (foreign key) data.

### Prerequisite
1. This tool has code to work with tab delimitted csv files.
*****if needed, please modify the code logic of reading/writing part accordingly*****
2. In case of Parent and Child files, First column of Parent file must be Primary Key of data and the second file's first column must match with the same (It should be foreign key).
3. This tool code has default hard code value 50000 rows to read as one data set from parent file and process at a time. This must be adjusted based on machine enviroment (i.e. Memory size)

### Code Approaches
This project has two aprroaches of chinking files, ****Normal loop**** and In ****Parallel loop****. In case of Normal loop, every set of Parent data will be processed ton find matching 
child data and will be written to a file one by one (one set at a time). In case of Parallel loop, multiple set of parent data will be processed parallely to find matching child data
and will be written to a file one by one (one set at a time).
