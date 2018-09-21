package SalesCountry;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class SalesCountryReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
// base on MapReduceBase imp Reducer
//<Text,IntWritable> => data type of input key-value to the reducer
//output of mapper <text,intWritable>=input of reducer <text,intWritable>
//every reducer must have reduce()

//purpose of reducer: 
/*<United Arab Emirates, 1>, <United Arab Emirates, 1>, 
<United Arab Emirates, 1>,<United Arab Emirates, 1>,
 <United Arab Emirates, 1>, <United Arab Emirates, 1>.
This is given to reducer as <United Arab Emirates, {1,1,1,1,1,1}>
*/

//so <Text t_key, Iterator<IntWritable> values> ==> accept the argument of this form
//

	public void reduce(Text t_key, Iterator<IntWritable> values, OutputCollector<Text,IntWritable> output, Reporter reporter) throws IOException {
		Text key = t_key;
		int frequencyForCountry = 0;
		while (values.hasNext()) {
			// replace type of value with the actual type of our value
			IntWritable value = (IntWritable) values.next();
			frequencyForCountry += value.get();
			
		}
		output.collect(key, new IntWritable(frequencyForCountry));
	}
}
