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
 * @version V1.0.0
 * @Date 2013-03-07 名称：共同ipsecond
 *       数据源：共同ipfirst得出的数据 /data/ci/result/userchaincommonipsecond_tmp/ (ip|uid1,uid2) 
 *       输出：/data/ci/result/userchaincommonipfirst2_tmp/ (uid1_uid2,ci：1,192.168.1.2|192.168.1.3)
 *       说明：用户关系链合并数据用
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
							outKey.set(vVals[j] + keySepa + vVals[i]);
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
		private static String valSepa = ":";
		private static String prefix = "ci";
		
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
			outVal.set(prefix + valSepa + 1 + "," + sb.substring(0,sb.length() -1)); // ci：1,192.168.1.2|192.168.1.3
			context.write(outKey, outVal);
		}
	}

}
