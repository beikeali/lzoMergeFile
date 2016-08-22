package di.lzoFileMerge;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LzoFileMergeMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

	 @Override
	 protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		//直接读入
	 	context.write(new Text(value), NullWritable.get());
	 }
}