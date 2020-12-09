package four;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable>{

    IntWritable v = new IntWritable();
    Map<String,Integer> map=new HashMap<String,Integer>();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) 
            throws IOException, InterruptedException {
            
        int sum=0;
        // 1 累加求和
        for (IntWritable value : values) {
            
            sum+=value.get();
        }
//        v.set(sum);

//        // 2 写出
//        context.write(key, v);
    
        String k=key.toString();
        map.put(k, sum);

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        //这里将map.entrySet()转换成list
        List<Map.Entry<String,Integer>> list=new LinkedList<Map.Entry<String,Integer>>(map.entrySet());
        //通过比较器来实现排序
        Collections.sort(list, new Comparator<Map.Entry<String, Integer>>(){
            //升序排序
            @Override
            public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                return (int)(o2.getValue()-o1.getValue());
            }
        });
        
        for(int i=0;i<100;i++){
            context.write(new Text(list.get(i).getKey()), new IntWritable(list.get(i).getValue()));
        }
    }    
}