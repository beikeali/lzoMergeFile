package di.lzoFileMerge;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.*;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.hadoop.compression.lzo.LzoIndexer;
import com.hadoop.compression.lzo.LzopCodec;
import com.hadoop.mapreduce.LzoTextInputFormat;
import org.apache.log4j.Logger;


public class Driver {
	
	static int RecudePerSize = 256*1024*1024;
	private final static Logger logger = Logger.getLogger(Driver.class);
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		 //输入三个参数 输入路径 输出路径 job名字
		 Configuration conf = new Configuration();
		 @SuppressWarnings("deprecation")
		 String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		 
		 if (otherArgs.length < 1) {
			 System.err.println("Usage: $hadoop jar /data/udcanalyze/udc/common/jar/lzoFileMerge.jar inputPath reducerSize(M)");
			 System.exit(2);
		 }
		 String inputStr = otherArgs[0];
		 String tmpStr = inputStr.replace("/user/www/", "/user/www/tmp/");
		 String jobName = "lzoFileMerge";
		 int reducePerSize = 0;
		 
		 if (otherArgs.length == 1){
			 reducePerSize = RecudePerSize;
		 }else{
			 reducePerSize = Integer.valueOf(otherArgs[1])*1024*1024;
		 }
		 // 文件输出目录，去掉结尾用户可能输入的 *
		 Path inPath = new Path(inputStr.replace("*", ""));
		 FileSystem fs = FileSystem.get(conf);
		 //tmpPath 中间结果数据目录，去掉结尾用户可能输入的 *
		 Path tmpPath = new Path(tmpStr.replace("*", ""));
		 logger.info("tmpPath dir : " + tmpPath);
		 // 删除已经存在的路径
		 if (fs.exists(tmpPath)) {
			 fs.delete(tmpPath, true);
		     logger.warn("delete existing tmpPath dir : " + tmpPath);
		 }
		 // 配置并运行job，解决lzo文件合并的job
		 Job job = new Job(conf, jobName);
		 job.setJarByClass(LzoFileMergeMapper.class);
		 // 设置输入lzo格式文件
		 job.setInputFormatClass(LzoTextInputFormat.class);
		 // 输入路径
		 FileInputFormat.addInputPath(job, new Path(inputStr));
		 //设置Mapper
		 job.setMapperClass(LzoFileMergeMapper.class);
		 // 设置输出类型 [应该和Recuder的输出一致]
		 job.setOutputKeyClass(Text.class);
		 job.setOutputValueClass(NullWritable.class);
		 job.setOutputFormatClass(TextOutputFormat.class);
		 //设置输出lzo压缩
		 TextOutputFormat.setCompressOutput(job, true);
		 TextOutputFormat.setOutputCompressorClass(job, LzopCodec.class);
		 // 设置reduce 这里的作用是用于控制最后输出文件的个数，避免小文件太多
		 job.setReducerClass(LzoFileMergeReducer.class);
		 // 设置reduce输出到临时路径
		 TextOutputFormat.setOutputPath(job, tmpPath);
		 //计算输入map数据量,兼容用户输入目录格式不为/*结尾的情况
		 String inputStrPattern = inputStr;
		 
		 if (!inputStrPattern.endsWith("/*")){
			 inputStrPattern = inputStrPattern + "/*";
		 }
		 logger.info("inputStrPattern: " + inputStrPattern);
		 //动态计算reduce数量
		 Path pattern = new Path(inputStrPattern);
		 FileSystem fs1 = FileSystem.get(conf);
		 FileStatus[] statuses = fs1.globStatus(pattern);
		 long totalInputSize = 0;
		 
		 for (FileStatus file : statuses) {
			 totalInputSize += file.getLen();
		 }
		 //动态得到reduce个数
		 int reduceTaskNum = (int) (totalInputSize/reducePerSize);
		 logger.info("totalInputSize: " + totalInputSize);
		 logger.info("RecudePerSize: " + reducePerSize);
		 logger.info("reduceTaskNum: " + reduceTaskNum);
		 
		 job.setNumReduceTasks(reduceTaskNum);
		 job.waitForCompletion(true);
		 
		 // 配置并运行job2，将合并好的数据从临时目录中写入到目标目录中（即原始的输入目录）
		 Job job2 = new Job(conf, jobName);
		 job2.setJarByClass(LzoFileMergeMapper.class);
		 // 设置输入lzo格式文件
		 job2.setInputFormatClass(LzoTextInputFormat.class);
		 // 输入路径
		 FileInputFormat.addInputPath(job2, tmpPath);
		 //设置Mapper
		 job2.setMapperClass(LzoFileMergeMapper.class);
		 // 设置输出类型 [应该和Recuder的输出一致]
		 job2.setOutputKeyClass(Text.class);
		 job2.setOutputValueClass(NullWritable.class);
		 job2.setOutputFormatClass(TextOutputFormat.class);
		 //设置输出lzo压缩
		 TextOutputFormat.setCompressOutput(job2, true);
		 TextOutputFormat.setOutputCompressorClass(job2, LzopCodec.class);
		 // 设置reduce 这里的作用是用于控制最后输出文件的个数，避免小文件太多
		 job2.setReducerClass(LzoFileMergeReducer.class);
		 // 设置reduce输出到临时路径
		 TextOutputFormat.setOutputPath(job2, inPath);
		 job2.setNumReduceTasks(reduceTaskNum);
		 
		 // 删除目标路径数据
		 if (fs.exists(inPath)) {
			 fs.delete(inPath, true);
		     logger.warn("delete existing inPath dir : " + inPath);
		 }
		 
		 int flag = job2.waitForCompletion(true) ? 0 : 1 ;
		 // 对输出路径进行构建lzo索引&删除临时目录
		 if (flag == 0) {
			 // 删除中间数据
			 if (fs.exists(tmpPath)) {
				 fs.delete(tmpPath, true);
			     logger.warn("delete existing tmpPath dir : " + tmpPath);
			     //fs.mkdirs(tmpPath);
			 }
			 
			 LzoIndexer index = new LzoIndexer(conf);
			 try {
				 index.index(inPath);
			 } catch (IOException e) {
				 e.printStackTrace();
			 }
		 }
		 	System.exit(flag);
		 }

}
