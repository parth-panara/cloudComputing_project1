// Following code is for EC2_A instance of AWS

package net.cloud;


import software.amazon.awssdk.services.rekognition.RekognitionClient; // Rekognition Client
import software.amazon.awssdk.services.rekognition.model.*; // AWS(S3Object, TextDetection, TextTypes, DetectTextRequest ,Image, DetectTextResponse)

import software.amazon.awssdk.services.s3.S3Client; // S3 Bucket Client
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.model.S3Object; 

import software.amazon.awssdk.services.sqs.SqsClient; //SQS client
import software.amazon.awssdk.services.sqs.model.*; // AWS(ListQueuesResponse, Message, DeleteMessageRequest, GetQueueUrlRequest,QueueNameExistsException, ReceiveMessageRequest, ListQueuesRequest) 

import software.amazon.awssdk.regions.Region; //AWS Region = US_EAST_1
import java.util.*;

public class AutomobileDetect_EC2A {


    public static void img_loading_Bucket(S3Client aws_s3, RekognitionClient aws_rek, SqsClient aws_sqs,
                                           String myBucketName, String myQueueName, String myQueueGroup) {

        // If a queue already exists, get the queueUrl.
        String queueUrl = "";
        try {
            ListQueuesRequest queueRequest = ListQueuesRequest.builder().queueNamePrefix(myQueueName).build();
            ListQueuesResponse queueResponse = aws_sqs.listQueues(queueRequest);

            if (queueResponse.queueUrls().size() == 0) {
                CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                        .attributesWithStrings(Map.of("FifoQueue", "true", "ContentBasedDeduplication", "true"))
                        .queueName(myQueueName)
                        .build();
                aws_sqs.createQueue(createQueueRequest);

                GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder().queueName(myQueueName).build();
                queueUrl = aws_sqs.getQueueUrl(getQueueUrlRequest).queueUrl();
            } else {
                queueUrl = queueResponse.queueUrls().get(0);
            }
        } catch (QueueNameExistsException e) {
            throw e;
        }

        // loading the given number of pictures in the bucket...
        try {
            ListObjectsV2Request listObjectsRequest = ListObjectsV2Request.builder().bucket(myBucketName).maxKeys(10).build();
            ListObjectsV2Response listObjectsResponse = aws_s3.listObjectsV2(listObjectsRequest);

            for (S3Object s3Object : listObjectsResponse.contents()) {
                System.out.println(".....loading image in S3 bucket(njit-cs-643): " + s3Object.key());

                Image image = Image.builder().s3Object(software.amazon.awssdk.services.rekognition.model.S3Object.builder().bucket(myBucketName).name(s3Object.key()).build())
                        .build();
                DetectLabelsRequest detectLabelsRequest = DetectLabelsRequest.builder().image(image).minConfidence((float) 90)
                        .build();
                DetectLabelsResponse detectLabelsResponse = aws_rek.detectLabels(detectLabelsRequest);
                List<Label> labels = detectLabelsResponse.labels();

                for (Label label : labels) {
                    if (label.name().equals("Car")) {
                        SendMessageRequest sendMessageRequest = SendMessageRequest.builder().messageGroupId(myQueueGroup).queueUrl(queueUrl)
                                .messageBody(s3Object.key()).build();
                        aws_sqs.sendMessage(sendMessageRequest);
                        break;
                    }
                }
            }

        
        
            //Sending "-1" to the queue signifies the completion of picture's filtering.
            aws_sqs.sendMessage(SendMessageRequest.builder().queueUrl(queueUrl).messageGroupId(myQueueGroup).messageBody("-1")
                    .build());
        } catch (Exception e) {
            System.err.println(e.getLocalizedMessage());
            System.exit(1);
        }
    }
    public static void main(String[] args) {

        String myBucketName = "njit-cs-643";
        String myQueueName = "car.fifo"; 
        String myQueueGroup = "group1";

        S3Client aws_s3 = S3Client.builder().region(Region.US_EAST_1).build();
        RekognitionClient aws_rek = RekognitionClient.builder().region(Region.US_EAST_1).build();
        SqsClient aws_sqs = SqsClient.builder().region(Region.US_EAST_1).build();

        img_loading_Bucket(aws_s3, aws_rek, aws_sqs, myBucketName, myQueueName, myQueueGroup);
    }

}
                    


// AWS Toolkit code references: https://github.com/awsdocs/aws-doc-sdk-examples/blob/master/javav2/example_code/