package four;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
    Text k = new Text();
    IntWritable v = new IntWritable(1);
    @Override
    protected void map(LongWritable key, Text value, Context context) 
            throws IOException, InterruptedException {
        //1 获取一行
        String line = value.toString();
        
        //2 切割单词
        String[] fields = line.split(",");
        
        //3 写出（1为购买，0为点击）
       
        //if(fields[3]=="1"| fields[3]=="2"|fields[3]=="3") {
         String str="";
         for (int i=0;i<fields.length;i++)
            if(i==3)
                if(Integer.parseInt(fields[i])<4&Integer.parseInt(fields[i])>0){
                       str=fields[i-1];
                        k.set(str);
                        context.write(k, v);
        } 
  
    }
}