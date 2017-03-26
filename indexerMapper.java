
package com.hadoop.example;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class indexerMapper extends Mapper<LongWritable, Text, Text, Text> {
  procedure MAP(did id, doc d) {
    H new AssociativeArray forall term t in doc d do
      H{t <-- ßH{t} + 1
    forall term t in H do
      EMIT( tuple <t, d> , tf H{t} )
  }
}
