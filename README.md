# tinygoogle-hadoop
Repository for Project 1 of CS 1699: Special Topics in Computer Science - Cloud Computing

## Project 1 - Hadoop

This is an implementation of a tinyGoogle search engine that readings in text files and allows a user to search for term(s) 
and get an output of the documents that contain each term ordered by the count in descending order. This also has the functionality to
add and index new text files too.

### Implementation

We implemented this project using Apache Hadoop MapReduce. We used two MapReduce steps to read in data from every file and generate an inverted index. We were then able to use this inverted index to search for which documents contained the highest count of each term. 
