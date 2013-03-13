package com.feinno.ci.scheduled.calculation.mr.userchain;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @version V1.0.0
 * @Date 2013-03-07 名称：共同ipfisrt2
 *       数据源：共同ipfirst得出的数据 /data/ci/result/userchaincommonipfirst_tmp/ (uid|ip) 
 *       输出：/data/ci/result/userchaincommonipfirst2_tmp/ (ip|uid1,uid2)
 *       说明：用户关系链合并数据用
 */
public class UserCommonIpFirst2
{
	public static class UserCommonIpFirstMapper extends
			Mapper<LongWritable, Text, Text, Text>
	{
		private Text outKey = new Text();
		private Text outVal = new Text();
		private String Spea = "\\|";
		String [] vals= null;
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException
		{
			vals = value.toString().split(Spea);
			if(vals.length == 2){
				outKey.set(vals[1]);//ip
				outVal.set(vals[0]);//uid
				context.write(outKey, outVal);
				
			}
		}
	}
	
	public static class UserCommonIpFirstReducer extends  Reducer<Text,Text,Text,Text>{
		private Text outKey = new Text();
		private Text outVal = new Text();
		StringBuffer sb = new StringBuffer();
		private String Spea = ",";
		public void reduce(Text key,Iterable<Text> values,Context context)throws IOException, InterruptedException{
			sb.setLength(0);
			for(Text t :values){
				sb.append(t.toString()).append(Spea);
			}
			outKey.set(key);
			outVal.set(sb.substring(0,sb.length()-1));
			context.write(outKey, outVal);//ip|uid,uid
		}
	}

}
