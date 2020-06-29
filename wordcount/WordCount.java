import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class WordCount {
        public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
                // 统计词频时，需要去掉标点符号等符号，此处定义表达式
                private String pattern = "[^a-zA-Z0-9-]";
                @Override
                protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                        // 将每一行转化为一个String
                        String line = value.toString();
                        // 将标点符号等字符用空格替换，这样仅剩单词
                        line = line.replaceAll(pattern, " ");
                        // 将String划分为一个个的单词
                        String[] words = line.split("\\s+");
                        // 将每一个单词初始化为词频为1，如果word相同，会传递给Reducer做进一步的操作
                        for (String word : words) {
                                if (word.length() > 0) {
                                        context.write(new Text(word), new IntWritable(1));
                                }
                        }
                }
        }
        public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
                @Override
                protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
                        // 初始化词频总数为0
                        Integer count = 0;
                        // 对key是一样的单词，执行词频汇总操作，也就是同样的单词，若是再出现则词频累加
                        for (IntWritable value : values) {
                                count += value.get();
                        }
                        // 最后输出汇总后的结果，注意输出时，每个单词只会输出一次，紧跟着该单词的词频
                        context.write(key, new IntWritable(count));
                }
        }
        public static void main(String[] args) throws Exception {
                // 以下部分为HadoopMapreduce主程序的写法，对照即可
                // 创建配置对象
                Configuration conf = new Configuration();
                // 创建Job对象
                Job job = Job.getInstance(conf, "wordcount");
                // 设置运行Job的类
                job.setJarByClass(WordCount.class);
                // 设置Mapper类
                job.setMapperClass(WordCountMapper.class);
                // 设置Reducer类
                job.setReducerClass(WordCountReducer.class);
                // 设置Map输出的Key value
                job.setMapOutputKeyClass(Text.class);
                job.setOutputValueClass(IntWritable.class);
                // 设置Reduce输出的Key value
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(IntWritable.class);
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