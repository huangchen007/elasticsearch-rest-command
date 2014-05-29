package com.everdata.test;


import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.AbstractHttpClient;
import org.apache.http.impl.client.DefaultHttpClient;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.SocketException;
import java.net.URL;
import java.net.URLConnection;


public class HttpHelper {

    public static String sendPostUrl(String url, String content) {
        try {
            DefaultHttpClient httpclient = new DefaultHttpClient();
            HttpPost httppost = new HttpPost(url);
//		content = content.replaceAll("__", "_");
            StringEntity reqEntity = new StringEntity(content, "UTF-8");
            httppost.setEntity(reqEntity);

            HttpResponse response = httpclient.execute(httppost);

            StringBuffer buffer = new StringBuffer();
            try {
                BufferedReader br = new BufferedReader(new InputStreamReader(response.getEntity().getContent(), "UTF-8"));
                String temp;
                while ((temp = br.readLine()) != null) {
                    buffer.append(temp);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            return buffer.toString();

        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }

    public static String getHttpDownload(String url) {
        int nStartPos = 0;
        int nRead = 0;
        String sName = url.substring(url.lastIndexOf("/") + 1, url.length());
//        String sPath = EsConfigUtil.getBasethPath() + File.separator + "down" + File.separator;
        String sPath = null;
        File file = new File(sPath);
        if (!file.exists()) {
            file.mkdirs();
        }
        try {
            URL httpurl = new URL(url);
            //打开连接
            HttpURLConnection httpConnection = (HttpURLConnection) httpurl
                    .openConnection();
            //获得文件长度
            long nEndPos = getFileSize(url);
            RandomAccessFile oSavedFile = new RandomAccessFile(sPath + sName + "", "rw");
            System.out.println("===" + sPath + sName);
            httpConnection.setRequestProperty("User-Agent", "Internet Explorer");
            String sProperty = "bytes=" + nStartPos + "-";
            //告诉服务器book.rar这个文件从nStartPos字节开始传
            httpConnection.setRequestProperty("RANGE", sProperty);
            System.out.println(sProperty);
            InputStream input = httpConnection.getInputStream();
            byte[] b = new byte[1024];
            //读取网络文件,写入指定的文件中
            while ((nRead = input.read(b, 0, 1024)) > 0
                    && nStartPos < nEndPos) {
                oSavedFile.write(b, 0, nRead);
                nStartPos += nRead;
            }
            httpConnection.disconnect();
        } catch (Exception e) {
            e.printStackTrace();
            return "";
        }
        return sName;
    }

    /**
     * 读取文件内容
     */
    public static String getHttpDownload(String url, String baseFolder) {
        // TODO Auto-generated method stub
        byte buffer[] = null;

        int byteread = 0;
        int bytesum = 0;

        StringBuffer sb = new StringBuffer();
        try {

            AbstractHttpClient httpclient = new DefaultHttpClient();// 创建一个客户端，类似打开一个浏览器
            httpclient.getParams().setParameter("http.connection.timeout", 100000);
            HttpGet get = new HttpGet(url);// 创建一个get方法，类似在浏览器地址栏中输入一个地址

            get.setHeader("Accept", "Accept text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8");
            get.setHeader("Accept-Language", "zh-cn,zh;q=0.5");
            get.setHeader("Connection", "keep-alive");
            get.setHeader("User-Agent", "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/29.0.1547.66 Safari/537.36");
            long t = System.currentTimeMillis();
            HttpResponse httpResponse = httpclient.execute(get);
            System.out.println(url + "请求结束:" + (System.currentTimeMillis() - t));
            int statusCode = httpResponse.getStatusLine()
                    .getStatusCode();
            if (HttpStatus.SC_OK == statusCode) {
                HttpEntity ent = httpResponse.getEntity();
                buffer = new byte[1024];
                InputStream in = ent.getContent();
//				fo=new FileOutputStream(new File(baseFolder+File.separator+filename),true);
                while ((byteread = in.read(buffer)) != -1) {
                    bytesum += byteread;
                    sb.append(new String(buffer, 0, byteread, "UTF-8"));
//					fo.write(buffer, 0, byteread);
                }
//				fo.close();


            }

        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
        }
        return sb.toString();
    }

    /**
     * 下载文件
     */
    public static String getHttpDownloadFile(String url, String baseFolder) {
        // TODO Auto-generated method stub
        byte buffer[] = null;

        int byteread = 0;
        int bytesum = 0;
        FileOutputStream fo = null;
        StringBuffer sb = new StringBuffer();
        String fileName = System.currentTimeMillis() + ".json";
        try {

            AbstractHttpClient httpclient = new DefaultHttpClient();// 创建一个客户端，类似打开一个浏览器
            httpclient.getParams().setParameter("http.connection.timeout", 100000);
            HttpGet get = new HttpGet(url);// 创建一个get方法，类似在浏览器地址栏中输入一个地址

            get.setHeader("Accept", "Accept text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8");
            get.setHeader("Accept-Language", "zh-cn,zh;q=0.5");
            get.setHeader("Connection", "keep-alive");
            get.setHeader("User-Agent", "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/29.0.1547.66 Safari/537.36");
            long t = System.currentTimeMillis();
            HttpResponse httpResponse = httpclient.execute(get);
            System.out.println(url + "请求结束:" + (System.currentTimeMillis() - t));
            int statusCode = httpResponse.getStatusLine()
                    .getStatusCode();
            if (HttpStatus.SC_OK == statusCode) {
                HttpEntity ent = httpResponse.getEntity();
                buffer = new byte[1024];
                InputStream in = ent.getContent();
                fo = new FileOutputStream(new File(baseFolder + File.separator + fileName), true);
                while ((byteread = in.read(buffer)) != -1) {
                    bytesum += byteread;
//					sb.append(new String(buffer, 0, byteread,"UTF-8"));
                    fo.write(buffer, 0, byteread);
                }
                fo.close();


            }

        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
        }
        return sb.toString();
    }

    public static long getFileSize(String sURL) {
        int nFileLength = -1;
        try {
            URL url = new URL(sURL);
            HttpURLConnection httpConnection = (HttpURLConnection) url
                    .openConnection();
            //	httpConnection.setRequestProperty("User-Agent", "Internet Explorer");
            int responseCode = httpConnection.getResponseCode();
            if (responseCode >= 400) {
                System.err.println("Error Code : " + responseCode);
                return -2; // -2 represent access is error
            }
            String sHeader;
            for (int i = 1; ; i++) {
                sHeader = httpConnection.getHeaderFieldKey(i);
                if (sHeader != null) {
                    if (sHeader.equals("Content-Length")) {
                        nFileLength = Integer.parseInt(httpConnection
                                .getHeaderField(sHeader));
                        break;
                    }
                } else
                    break;
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(nFileLength);
        return nFileLength;
    }

    public static String sendGetUrl(String host, String uri) {
        
        try {  
            StringBuffer html = new StringBuffer();  
            URL url = new URL(host+uri);  
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();  
            
           
            conn.connect();
            conn.setReadTimeout(500);
            
            //success_conn++
            CommandActionTest.success_conn.incrementAndGet();


            InputStreamReader isr = new InputStreamReader(conn.getInputStream(), "utf-8");  
            BufferedReader br = new BufferedReader(isr);  
            String temp;
            while ((temp = br.readLine()) != null) {  
                html.append(temp).append("\n");
            }  
            br.close();  
            isr.close();
            
            conn.disconnect();
            return html.toString();  
         } catch (SocketException e) {  
        	 //System.out.println();        	 
         } catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        return null;

    }

    public static String getRequest(String host) {
        return null;
    }
}

