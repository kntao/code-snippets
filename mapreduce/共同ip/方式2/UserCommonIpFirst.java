package com.feinno.ci.scheduled.calculation.mr.userchain;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 * 用户关系链需要基础数据-共同IP 数据源：登录日志 计算得出：ip|uid1,uid2,uid3
 * 
 * @author kongqingtao since 2013-03-07
 */
public class UserCommonIpFirst
{
	public static class UserCommonIpFirstMapper extends
			Mapper<LongWritable, Text, Text, Text>
	{
		private Text outKey = new Text();
		private Text outVal = new Text();
		
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException
		{
			String [] vals = value.toString().split(",");
			if(vals.length == 3){
				if(null != vals[2] && vals[2].length() > 0){
					outKey.set(vals[2]);
					outVal.set(vals[0]);
					context.write(outKey, outVal);
				}
			}
		}
	}
	
	public static class UserCommonIpFirstReducer extends  Reducer<Text,Text,Text,Text>{
		private Set<String> sets = new HashSet<String>();
		private Text outKey = new Text();
		private Text outVal = new Text();
		private StringBuffer sb = new StringBuffer();
		
		public void reduce(Text key,Iterable<Text> values,Context context)throws IOException, InterruptedException{
			Iterator<Text> iter = values.iterator();
			sets.clear();
			sb.setLength(0);
			
			while(iter.hasNext()){
				String val = iter.next().toString();
				if(UserChainFirst.isUid(val)){
					sets.add(val);
				}
			}
		
			for(String t : sets){
				sb.append(t);
				sb.append(",");
			}
			outKey.set(key);
			outVal.set(sb.substring(0,sb.length() -1));
			context.write(outKey, outVal);
		}
	}

}
