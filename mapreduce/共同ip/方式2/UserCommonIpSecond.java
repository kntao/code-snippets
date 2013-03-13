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
 * 用户关系链需要基础数据-共同IP 数据源：共同ip结果1(UserCommonIpFirst) 计算得出：uid1_uid2,ip1|ip2
 * 
 * @author kongqingtao since 2013-03-07
 */
public class UserCommonIpSecond
{
	public static class UserCommonIpSecondMapper extends
			Mapper<LongWritable, Text, Text, Text>
	{
		private Text outKey = new Text();
		private Text outVal = new Text();
		private static String sepa = "\\|";
		private static String keySepa = "_";
		
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException
		{
			String [] vals = value.toString().split(sepa);
			if(vals.length == 2 && null != vals[1]){
				String [] vVals = vals[1].split(",");
				if(vVals.length > 1){ // 笛卡尔积 转uid1_uid2, ip
					for(int i = 0; i< vVals.length;i++)
					{
						for(int j = i+1; j<vVals.length;j++){
							outKey.set(vVals[i] + keySepa + vVals[j]);
							outVal.set(vals[0]);
							context.write(outKey, outVal);
						}
					}
				}
			}
		}
	}
	
	public static class UserCommonIpSecondReducer extends  Reducer<Text,Text,Text,Text>{
		private Text outKey = new Text();
		private Text outVal = new Text();
		private StringBuffer sb = new StringBuffer();
		
		public void reduce(Text key,Iterable<Text> values,Context context)throws IOException, InterruptedException{
			Iterator<Text> iter = values.iterator();
			sb.setLength(0);
			
			while(iter.hasNext()){
				String val = iter.next().toString();
				sb.append(val);
				sb.append("|");
			}
		
			outKey.set(key);
			outVal.set(sb.substring(0,sb.length() -1));
			context.write(outKey, outVal);
		}
	}

}
