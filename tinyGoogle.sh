#! /bin/bash
clear
hadoop com.sun.tools.javac.Main *.java && jar cf tinyGoogle.jar *.class && rm -rf /hadoop/projects/hdfs/output &&hadoop jar tinyGoogle.jar tinyGoogle /hadoop/projects/hdfs/input /hadoop/projects/hdfs/output
