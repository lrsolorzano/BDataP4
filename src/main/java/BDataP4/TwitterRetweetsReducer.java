package BDataP4;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by lsolorzano on 3/24/2017.
 */
public class TwitterRetweetsReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        java.lang.StringBuilder listado= new java.lang.StringBuilder();
        for (Text value : values){
            listado.append("," + value.toString());
        }
        context.write(key, new Text(listado.toString().toString()));
    }
}