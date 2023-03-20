package com.test.dataengineer

import com.amazonaws.auth.AWSCredentialsProviderChain
import com.amazonaws.auth.profile.ProfileCredentialsProvider

object TestApp {

  def main(args: Array[String]) = {

    val usage =
      """
        |Usage: {inputFilePath} {outputFilePath} {awsProfileName}
        |""".stripMargin

    /**
     * Handling the command line arguments used while submitting the job
     * if user not provide the required arguments then giving error straight way
     * if required arguments passed, code processed in 4 abstract steps
     * 1. create spark session
     * 2. read input files
     * 3. transformed the read dataframe
     * 4. writing the transformed dataframe
     */
    args.toList match {
      case _ :: Nil => throw new RuntimeException("output file path is not provided")
      case _ :: _ :: Nil => throw new RuntimeException("aws profile name is not provided")
      case input :: output :: profile :: _ =>
        val (accessKey, secretKey) = if (profile == "local") ("", "") else getAwsCredentials(profile)
        implicit val spark = DfProcessor.createSparkSession(accessKey, secretKey)
        val inputDF = DfProcessor.readInputFiles(input)
        val processedDF = DfProcessor.processDF(inputDF)

        DfProcessor.writeFile(output, processedDF)

        println(s"Successfully write the output to the path => $output")
        spark.stop()
      case _ => throw new RuntimeException(usage)
    }
  }

  /**
   *  Get the AwsAccessKey and SecretKey from the profile
   * @param profile
   * @return
   */
  private def getAwsCredentials(profile: String) = {
    val awsCredentials = new AWSCredentialsProviderChain(
      new ProfileCredentialsProvider(profile))
    (awsCredentials.getCredentials.getAWSAccessKeyId,
      awsCredentials.getCredentials.getAWSSecretKey)
  }

}


