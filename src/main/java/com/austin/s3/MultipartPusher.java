package com.austin.s3;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;

public class MultipartPusher {
	AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withRegion(Regions.US_EAST_1)
			.withCredentials(new ProfileCredentialsProvider()).build();
	List<PartETag> partETags = new ArrayList<>();
	InitiateMultipartUploadResult initResponse;
	private String bucketName;
	private String keyName;
	private String encoding;
	//Amazon requires you start at 1
	private int partNumber = 1;

	public MultipartPusher(String bucketName, String keyName,String encoding) {
		this.bucketName = bucketName;
		this.keyName = keyName;
		this.encoding=encoding;
		InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(bucketName, keyName);
		initResponse = s3Client.initiateMultipartUpload(initRequest);
	}

	public void pub(InputStream input) throws IOException {
		try{
			ObjectMetadata om=	new ObjectMetadata();
			om.setContentEncoding(this.encoding);
			UploadPartRequest uploadRequest = new UploadPartRequest().withBucketName(bucketName).withKey(keyName)
				.withUploadId(initResponse.getUploadId()).withPartNumber(partNumber++).withInputStream(input)
				.withPartSize(input.available()).withObjectMetadata(om);

		UploadPartResult uploadResult = s3Client.uploadPart(uploadRequest);
		partETags.add(uploadResult.getPartETag());}
		catch(Exception e) {
			e.printStackTrace();
		}

	}

	public void close() {
		CompleteMultipartUploadRequest compRequest = new CompleteMultipartUploadRequest(bucketName, keyName,
				initResponse.getUploadId(), partETags);
		s3Client.completeMultipartUpload(compRequest);
	}
}