# DataEngineerTest
### To run this example in local mode but want to use S3, use the following commands
1. sbt 
2. run {s3_input_file_path} {s3_output_file_path} {aws_profile_name}
aws_profile_name which is set up in your ~/.aws/credentials
eg command:- run s3a://spark-test-files/input-files s3a://spark-test-files/output-files/ default

### To run this example in spark local mode with local file system, use the following commands
1. sbt
2. run {input_file_path} {output_file_path} local

### Runtime parameter description below
1. input_file_path
2. output_file_path
3. local or {aws_profile_name} => if 3rd argument local that means read and write file from local file system else it will 
read accessKey and secretKey from mentioned profile.

### To run on EMR cluster use jar file which will be created after running below command
- sbt assembly
- once assembly completed it will create jar file in target/scala-2.13/DataEngineerTest-assembly-0.1.0-SNAPSHOT.jar
- Jar file you can use it to run on EMR cluster or in your own spark cluster.