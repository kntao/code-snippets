package com.feinno.ci.scheduled.calculation.mr.userchain;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 * @version V1.0.0
 * @Date 2013-03-07 名称：共同ipfisrt
 *       数据源：用户登录日志 /data/ci/stg/ip/ (用户UID,登录时间,登录ip） 
 *       输出：/data/ci/result/userchaincommonipfirst_tmp/ (uid|ip)
 *       说明：用户关系链合并数据用
 */
public class UserCommonIpFirst
{
	public static class UserCommonIpFirstMapper extends
			Mapper<LongWritable, Text, Text, Text>
	{
		private Text outKey = new Text();
		private Text outVal = new Text();
		private String Spea = "|";
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException
		{
			String [] vals = value.toString().split(",");
			if(vals.length == 3){
				if(null != vals[2] && vals[2].length() > 0 && UserChainFirst.isUid(vals[0])){
					outKey.set(vals[0]+Spea + vals[2]);//uid|ip
					outVal.set("1");
					context.write(outKey, outVal);
				}
			}
		}
	}
	
	public static class UserCommonIpFirstReducer extends  Reducer<Text,Text,Text,Text>{
		private Text outKey = new Text();
		public void reduce(Text key,Iterable<Text> values,Context context)throws IOException, InterruptedException{
			outKey.set(key);
			context.write(outKey, null);//uid|ip
		}
	}

}
