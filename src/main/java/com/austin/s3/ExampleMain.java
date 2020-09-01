package com.austin.s3;


import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;

public class ExampleMain {
    static String bucketName = System.getProperty("bucket-name"); // Need an s3 bucket
    public static void main(String[] args) throws IOException {

        readStock();
        readImage();
    }

    public static void readInputStreamToS3(InputStream inputStream, String bucketName, String keyName,String encoding) throws IOException {
        S3multipartOutputStream s3multipartOutputStream=new S3multipartOutputStream(new MultipartPusher(bucketName, keyName,encoding),5242880);
        byte[] buffer =new byte[255];
        int length;
        while((length = inputStream.read(buffer,0,buffer.length))>0){
            s3multipartOutputStream.write(buffer,0,length);
        }
        s3multipartOutputStream.close();
    }

    private static void readStock() throws IOException {
        String urlStr = "https://www.nasdaq.com/api/v1/historical/AAPL/stocks/2011-01-01/2020-01-01";
        HttpURLConnection in = (HttpURLConnection)(new URL(urlStr).openConnection());
        in.setRequestProperty("User-Agent","Test/7.25.0");
        readInputStreamToS3(in.getInputStream(),bucketName, "web.csv","identity");
    }
    private static void readImage() throws IOException {
        String urlStr = "https://i.pinimg.com/originals/8a/41/eb/8a41ebc63b3601d142338d5184afb906.jpg";
        HttpURLConnection in = (HttpURLConnection)(new URL(urlStr).openConnection());
        in.setRequestProperty("User-Agent","Test/7.25.0");

        readInputStreamToS3(in.getInputStream(),bucketName, "img.jpg","identity");
    }
}
