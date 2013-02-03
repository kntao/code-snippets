package com.xxx.xxx.query;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.io.IOUtils;

/*
 * ����ԭ���ݷ���������
 * @author kongqingtao
 * �߳�T1
 */
public class HandleRawDataThread extends Thread
{
	private LinkedBlockingQueue<String> rawDataQueue; // ����Q1
	
	public HandleRawDataThread(LinkedBlockingQueue<String> lbQueue){
		if(lbQueue == null)
			lbQueue = new LinkedBlockingQueue<String>();
		this.rawDataQueue = lbQueue;
	}
	public void run(){
		String rawDataPath = null;
		String rawFileExt = null;
		String rawFileExtAfterRead = null;
		List<String> fileLists = null;
		rawDataPath = Factory.getSystemPropertiesValue("rawDataPath");
		rawFileExt = Factory.getSystemPropertiesValue("fileExtention");
		rawFileExtAfterRead = Factory.getSystemPropertiesValue("rawFileExtentionAfterRead");
		while(true){
			fileLists = FileOperation.listAllFiles(rawDataPath, rawFileExt);
			
			for(String path : fileLists){
				BufferedReader in = null;
				try
				{
				    in = new BufferedReader(new InputStreamReader
				    		(new FileInputStream(path),"UTF-8"));
					String line;
					while ((line = in.readLine()) != null) // ���ж���
					{
						if(null != line && line.length() != 0){
							String[] uids = line.split(",");
							rawDataQueue.addAll(Arrays.asList(uids)); // �Ѷ�ȡ�����ŵ�Q1��
							System.out.printf("����ԭʼ����%s����%s\n", line,rawDataQueue.size());
						}
					}
					in.close();
					// ������
					String desPath = path.substring(0,path.lastIndexOf(rawFileExt))+ rawFileExtAfterRead;
					FileOperation.RenameFile(path, desPath);
				}
				catch (UnsupportedEncodingException e)
				{
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				catch (FileNotFoundException e)
				{
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				catch (IOException e)
				{
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				finally{	
					IOUtils.closeStream(in);
				}
			}
			// ����fileLists
			fileLists.clear();
			try
			{
				Thread.sleep(1*1000);
			}
			catch (InterruptedException e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public static void main(String[] args)
	{
		LinkedBlockingQueue<String> rawDataQueue = null;
		Thread t = new Thread(new HandleRawDataThread(rawDataQueue));
		t.start();
	}
}
