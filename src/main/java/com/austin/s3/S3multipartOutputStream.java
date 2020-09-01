package com.austin.s3;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class S3multipartOutputStream extends OutputStream {
	public List<Byte> bytes =new ArrayList<>();
	private MultipartPusher multipartPusher;
	private final int MAX_SIZE_TO_FLUSH;

	public S3multipartOutputStream(MultipartPusher multipartPusher,int MAX_SIZE_TO_FLUSH) {
		this.multipartPusher=multipartPusher;
		this.MAX_SIZE_TO_FLUSH=MAX_SIZE_TO_FLUSH;
	}
	@Override
	public void write(int b) throws IOException {
		bytes.add((byte)b);
		if(bytes.size()>MAX_SIZE_TO_FLUSH)
			writeToS3();
	}
	

	public void writeToS3() throws IOException{
		byte[]bytesArray=byteListTobyteArray(bytes);
		this.bytes.clear();
		System.out.println(bytesArray.length);
		multipartPusher.pub(new ByteArrayInputStream(bytesArray));
	}

	private byte[]byteListTobyteArray(List<Byte> byteList){
		byte[]sv=new byte[byteList.size()];
		for(int i = 0; i< byteList.size(); i++) {
			sv[i]= byteList.get(i);
		}

		return sv;

	}
	@Override
	public void close() throws IOException {
		this.writeToS3();
		multipartPusher.close();
	}
	
}
