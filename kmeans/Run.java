
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Run {

	private static String FLAG = "KCLUSTER";

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		Configuration configuration = new Configuration();
		Path kMeansPath = new Path("input/center.txt");   //初始的质心文件
		Path samplePath = new Path("input/samplecenters.txt");   //样本文件
		//加载聚类中心文件
		Center center = new Center();
		String centerString = center.loadInitCenter(kMeansPath);

		int index = 0;  //迭代次数
		while( index < 5){

			configuration = new Configuration();
			configuration.set(FLAG, centerString);  //将聚类中心的字符串放到configuration中

			kMeansPath = new Path("output/" + index); //本次迭代的输出路口，也是下一次质心的读取路径

			//判断输出路径是否存在，如果存在则删除
			FileSystem hdfs = FileSystem.get(configuration);
			if(hdfs.exists(kMeansPath))  hdfs.delete(kMeansPath, true);
			Job job = Job.getInstance(configuration, "kmeans" + index);
			job.setJarByClass(Run.class);
			job.setMapperClass(TokenizerMapper.class);
			job.setReducerClass(IntSumReducer.class);
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, samplePath);
	        FileOutputFormat.setOutputPath(job, kMeansPath);
	        job.waitForCompletion(true);

	        /*
	         * 获取自定义center大小，如果等于质心的大小，说明质心已经不会发生变化了，则程序停止迭代
	         */
	        long counter = job.getCounters().getGroup("myCounter").findCounter("kmeansCounter").getValue();
            if(counter == Center.k) System.exit(0);
            /**重新加载质心*/
            center = new Center();
            centerString = center.loadCenter(kMeansPath);
            index ++;
		}
		System.exit(0);
	}

}