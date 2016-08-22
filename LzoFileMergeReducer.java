package di.lzoFileMerge;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Created by zhaowanli on 16/8/20.
 * 功能：输出key
 */
public class LzoFileMergeReducer extends Reducer<Text,NullWritable,Text,NullWritable> {
	@Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
     //直接输出 
	 context.write(key, NullWritable.get());
    }
}
