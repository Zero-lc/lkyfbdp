package ff;

import org.apache.hadoop.io.WritableComparable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
public class UserTestWritable implements WritableComparable<UserTestWritable> {
    
    private StringBuffer val;
    private Text key;
    public UserTestWritable() {
    }
    public UserTestWritable(Text key, StringBuffer val) {
        this.key = key;
        this.val = val;
    }
    @Override
    public int compareTo(UserTestWritable o) {
        return 0;
    }
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(new String(key.copyBytes()));
        out.writeUTF(val.toString());
    }
    @Override
    public void readFields(DataInput in) throws IOException {
    }
    public Text getKey() {
        return key;
    }
    public void setKey(Text key) {
        this.key = key;
    }
    public StringBuffer getValue() {
        return val;
    }
    public void setValue(StringBuffer val) {
        this.val = val;
    }
    public String toString() {
        return  "("+key.toString()+"[" +(val.toString().substring(0,val.toString().length()-1))+"])" ;
    }
    
}