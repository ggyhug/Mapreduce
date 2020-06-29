import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;


public class SortedData {
    /**
     * 使用Mapper将数据文件中的数据本身作为Mapper输出的key直接输出
     */

    public static class forSortedMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
        private IntWritable mapperValue = new IntWritable(); // 存放key的值

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString(); // 获取读取的值，转化为String
            mapperValue.set(Integer.parseInt(line)); // 将String转化为Int类型
            context.write(mapperValue, new IntWritable(1)); // 将每一条记录标记为（key，value） key--数字 value--出现的次数
        }
    }

    /**
     * 使用Reducer将输入的key本身作为key直接输出
     */

    public static class forSortedReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        private IntWritable postion = new IntWritable(1); // 存放名次

        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            for (IntWritable item : values) { // 同一个数字可能出多次，就要多次并列排序
                context.write(postion, key); // 写入名次和具体数字
                System.out.println(postion + "\t" + key);
                postion = new IntWritable(postion.get() + 1); // 名次加1
            }
        }
    }

    public static void main(String[] args) throws Exception {
        // 创建配置对象
        Configuration conf = new Configuration();
        // 创建Job对象
        Job job = Job.getInstance(conf, "SortedData");
        // 设置运行Job的类
        job.setJarByClass(SortedData.class);
        // 设置处理map,reduce的类
        job.setMapperClass(forSortedMapper.class);
        job.setReducerClass(forSortedReducer.class);
        // 设置输入输出格式的处理
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        // 设定输入输出路径
        // 设置输入输出的路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // 提交job
        boolean b = job.waitForCompletion(true);
        if(!b) {
                System.out.println("Wordcount task fail!");
        }
    }

}
