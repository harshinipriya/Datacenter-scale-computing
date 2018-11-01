Lambda function invocation:
1. Created IAM role for the lambda function and attached two policies to it, "AmazonS3FullAccess" and "CloudWatchFullAccess".
2. Created a lambda function named lambda_trigger with python3 runtime and chose the role created from the previous step.
3. Added S3 trigger option from the Design window and chose trigger on 'ObjectCreated' and gave the bucket name for which the lambda function should be triggered.
4. Amazon S3 is one of the supported AWS event sources that can publish object-created events and invoke the Lambda function. 
5. In this project, the Lambda function code written can read the objects from the S3 bucket, process it(transform all letters to lowercase, strip non alphabetic characters and remove stop words), and then save it in another S3 bucket.

How lambda function works:
1. The application code is uploaded in the form of one or more Lambda functions to AWS Lambda. 
2. In turn, AWS Lambda executes the code on our behalf. AWS Lambda takes care of provisioning and managing the servers to run the code upon invocation.
3. We define an event source mapping, which is how we identify what events to track and which Lambda function to invoke. One example is using the bucket notification feature of S3. 
4. We can configure an event source mapping that directs Amazon S3 to invoke a Lambda function when a specific type of event occurs. The events in S3 are, A new object created event, An object removal event, A Reduced Redundancy Storage (RRS) object lost event.

What a lambda does:
1. AWS Lambda is a compute service that lets us run code without provisioning or managing servers. 
2. AWS Lambda executes our code only when needed and scales automatically, from a few requests per day to thousands per second. 
3. We pay only for the compute time we consume - there is no charge when our code is not running. 
4. With AWS Lambda, we can run code for virtually any type of application or backend service - all with zero administration. 
5. AWS Lambda runs our code on a high-availability compute infrastructure and performs all of the administration of the compute resources, including server and operating system maintenance, capacity provisioning and automatic scaling, code monitoring and logging. 
6. All we need to do is supply our code in one of the languages that AWS Lambda supports.


Lambda function integration with AWS framework:
1. The lambda function written for the project used S3 bucket notification system. The lambda function was invoked whenever a new file was created in the S3 bucket.
2. The lambda function used boto3 which is an AWS SDK for python. boto3 was invoked on the S3 service. 
3. If an event occured, the fileObject was retrieved from the event. The file name of the object created in the configured bucket is obtained using the file object.
4. The contents of the file are read as a string and processed by converting to lowercase and stripping non alphabetic and stop words.
5. A resource is created for the output bucket using boto3. An S3 object is created, into which the processed data is written and then uploaded into the bucket.



Reference:
1. docs.aws.amazon.com
2. boto3.amazonaws.com
3. https://cloudvedas.com/how-to-download-a-complete-s3-bucket-or-a-s3-folder/


