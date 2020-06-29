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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndex {

	//key:单词+文件地址  value:均为1
	public static class Map extends Mapper<Object,Text,Text,Text>{
        // 统计词频时，需要去掉标点符号等符号，此处定义表达式
        private String pattern = "[^a-zA-Z0-9-]";
		private Text keyStr = new Text();
		private Text value = new Text();
        private FileSplit fileSplit;
        @Override
		public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
			//获取输入文件信息
            fileSplit = (FileSplit)context.getInputSplit();
            // 将每一行转化为一个String
            String line = value.toString();
            // 将标点符号等字符用空格替换，这样仅剩单词
            line = line.replaceAll(pattern, " ");
            // 将String划分为一个个的单词
            String[] words = line.split("\\s+");
            // 将每一个单词初始化为词频为1，如果word相同，会传递给Reducer做进一步的操作
			for (String word : words){
                if(word.length()>0){
                    String pathname = fileSplit.getPath().getName().toString();
                    keyStr.set(word+":"+pathname);
                    value.set("1");
                    context.write(keyStr,value);
                }
			}
		}
	}
	//key:单词  value:地址+该地址频数
	public static class Combine extends Reducer<Text,Text,Text,Text>{
        private Text newvalue = new Text();
        @Override
		public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
            // 初始化词频总数为0
			int count = 0;
			// 对key是一样的单词，执行词频汇总操作，也就是同样的单词，若是再出现则词频累加
			for(Text value:values){
				count += Integer.parseInt(value.toString());
			}
			//拆分原有key，将单词作为新key,文件地址+频数 作为value
			int index = key.toString().indexOf(":");
			String word = key.toString().substring(0,index);
			String pathname = key.toString().substring(index+1,key.toString().length());
			key.set(word);
			newvalue.set("("+pathname+","+count+")");
			context.write(key,newvalue);
		}
    }

	//key:单词  value:不同地址+各自地址下的频数
	public static class Reduce extends Reducer<Text,Text,Text,Text>{
		Text newvalue = new Text();
		public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
            String files = "";
			for(Text value:values){
				files += value+",";
			}
			newvalue.set(files);
			context.write(key,newvalue);
		}
    }

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 以下部分为HadoopMapreduce主程序的写法，对照即可
        // 创建配置对象
		Configuration conf = new Configuration();
        // 创建Job对象
        Job job = new Job(conf,"invertedIndex");
        // 设置运行Job的类
        job.setJarByClass(InvertedIndex.class);
        // 设置Mapper类
        job.setMapperClass(Map.class);
        // 设置Combiner类
        job.setCombinerClass(Combine.class);
        // 设置Reducer类
        job.setReducerClass(Reduce.class);
        // 设置Reduce输出的Key value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // 设置输入输出的路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // 提交job
        boolean b = job.waitForCompletion(true);
        if(!b) {
                System.out.println("InvertedIndex task fail!");
        }
	}

}

