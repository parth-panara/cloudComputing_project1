// Following code is for EC2_B instance of AWS

package net.cloud;

import software.amazon.awssdk.services.rekognition.RekognitionClient;  // Rekognition Client
import software.amazon.awssdk.services.rekognition.model.*;  // AWS(S3Object, TextDetection, TextTypes, DetectTextRequest ,Image, DetectTextResponse)

import software.amazon.awssdk.services.s3.S3Client; // S3 Bucket Client

import software.amazon.awssdk.services.sqs.SqsClient; //SQS Client
import software.amazon.awssdk.services.sqs.model.*; // AWS(ListQueuesResponse, Message, DeleteMessageRequest, GetQueueUrlRequest,QueueNameExistsException, ReceiveMessageRequest, ListQueuesRequest) 

import software.amazon.awssdk.regions.Region; //AWS Region = US_EAST_1

import java.io.*;
import java.util.*;


public class AutomobileText_EC2B {

    public static void carImagesProcess(S3Client aws_s3, RekognitionClient aws_rek, SqsClient aws_sqs, String myBucketName, String myQueueName) {

        //DetectCars will poll SQS until the queue is established.
        boolean queueExists = false;
        while (!queueExists) {
            ListQueuesRequest requestListQueues = ListQueuesRequest.builder().queueNamePrefix(myQueueName)
                    .build();
            ListQueuesResponse responseListQueues = aws_sqs.listQueues(requestListQueues);
            if (responseListQueues.queueUrls().size() > 0)
                queueExists = true; 
        }

        // Obtain the URL of the queue.
        String queueUrl = "";
        try {
            GetQueueUrlRequest getRequestQueueUrl = GetQueueUrlRequest.builder().queueName(myQueueName).build();
            queueUrl = aws_sqs.getQueueUrl(getRequestQueueUrl).queueUrl();
        } catch (QueueNameExistsException e) {
            throw e;
        }
                    

        // Analyze each image of the cars
        try {
            boolean endOfQueue = false;
            HashMap<String, String> outputs = new HashMap<>();

            while (!endOfQueue) {
                // Obtain the index of the upcoming image
                ReceiveMessageRequest requestReceiveMessage =  ReceiveMessageRequest.builder().queueUrl(queueUrl).maxNumberOfMessages(1).build();
                        
                List<Message> messages = aws_sqs.receiveMessage(requestReceiveMessage).messages();

                if (messages.size() > 0) {
                    Message message = messages.get(0);
                    String label = message.body();

                    if (label.equals("-1")) {
                        //Upon completing its image processing, instance A appends the index -1 to the queue
                        // This signifies to instance B that there will be no further indexes added.
                        endOfQueue = true;
                    } else {
                        System.out.println("Collecting..... car picture including text from s3 bucket(njit-cs-643): " + label);

                        Image myImage = Image.builder().s3Object(S3Object.builder().bucket(myBucketName).name(label).build()).build();
                                
                        DetectTextRequest detect = DetectTextRequest.builder().image(myImage).build();
                                
                        DetectTextResponse output = aws_rek.detectText(detect);
                        List<TextDetection> textDetections = output.textDetections();

                        if (textDetections.size() != 0) {
                            String text = "";
                            for (TextDetection textDetection : textDetections) {
                                if (textDetection.type().equals(TextTypes.WORD))
                                    text = text.concat(" " + textDetection.detectedText());
                            }
                            outputs.put(label, text);
                        }
                    }

                    // Remove the processed message from the queue now that it has been dealt with.
                    DeleteMessageRequest requestDeleteMessage = DeleteMessageRequest.builder().queueUrl(queueUrl).receiptHandle(message.receiptHandle())
                            .build();
                    aws_sqs.deleteMessage(requestDeleteMessage);
                }
            }
            try {
                FileWriter writer = new FileWriter("myAppoutput.txt");

                Iterator<Map.Entry<String, String>> iterate = outputs.entrySet().iterator();
                while (iterate.hasNext()) {
                    Map.Entry<String, String> pair = iterate.next();
                    writer.write(pair.getKey() + ":" + pair.getValue() + "\n");
                    iterate.remove();
                }

                writer.close();
                System.out.println("Output sent to file myAppoutput.txt");
            } catch (IOException e) {
                System.out.println("An error occurred when writing to the file.");
                e.printStackTrace();
            }
        } catch (Exception e) {
            System.err.println(e.getLocalizedMessage());
            System.exit(1);
        }
    }
    
    public static void main(String[] args) {

        String myBucketName = "njit-cs-643";
        String myQueueName = "car.fifo"; 

        S3Client aws_s3 = S3Client.builder().region(Region.US_EAST_1).build();
        RekognitionClient aws_rek = RekognitionClient.builder().region(Region.US_EAST_1).build();
        SqsClient aws_sqs = SqsClient.builder().region(Region.US_EAST_1).build();
        
        carImagesProcess(aws_s3, aws_rek, aws_sqs, myBucketName, myQueueName);

    }
}



// AWS Toolkit code references: https://github.com/awsdocs/aws-doc-sdk-examples/blob/master/javav2/example_code/
    
                    
                    


