package com.derivative;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.util.UUID;
import java.util.Properties;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.s3.AmazonS3Client;
import org.apache.http.client.CredentialsProvider;

public class KinesisApplicationMain {

    public static String SAMPLE_APPLICATION_STREAM_NAME = null;
            //"OptionList";

    private static  String SAMPLE_APPLICATION_NAME = null;
            //"OptionApplication";

    // Initial position in the stream when the application starts up for the first time.
    // Position can be one of LATEST (most recent data) or TRIM_HORIZON (oldest available data)
    private static final InitialPositionInStream SAMPLE_APPLICATION_INITIAL_POSITION_IN_STREAM =
            InitialPositionInStream.LATEST;

    //private static ProfileCredentialsProvider credentialsProvider;
    private static ClasspathPropertiesFileCredentialsProvider credentialsProvider;


    private static void init() {

        loadConfig();
        // Ensure the JVM will refresh the cached IP values of AWS resources (e.g. service endpoints).
        java.security.Security.setProperty("networkaddress.cache.ttl", "60");

        /*
         * The ProfileCredentialsProvider will return your [default]
         * credential profile by reading from the credentials file located at
         * (~/.aws/credentials).
         */
       /* credentialsProvider = new ProfileCredentialsProvider();
        try {
            credentialsProvider.getCredentials().;
            System.out.println("credential");
        } catch (Exception e) {
            throw new AmazonClientException("Cannot load the credentials from the credential profiles file. "
                    + "Please make sure that your credentials file is at the correct "
                    + "location (~/.aws/credentials), and is in valid format.", e);
        }*/

        credentialsProvider =
                new ClasspathPropertiesFileCredentialsProvider();



        try {
            AWSCredentials credentials = credentialsProvider.getCredentials();



            System.out.println("credential");
        } catch (Exception e) {
            throw new AmazonClientException("Cannot load the credentials from the credential profiles file. "
                    + "Please make sure that your credentials file is at the correct "
                    + "location (~/.aws/credentials), and is in valid format.", e);
        }

    }

    private static void loadConfig() {

        try {

            InputStream input = KinesisApplicationMain.class.getClassLoader().
                    getResourceAsStream("config.properties");
            {

                Properties prop = new Properties();

                if (input == null) {
                    System.out.println("Sorry, unable to find config.properties");
                    return;
                }

                //load a properties file from class path, inside static method
                prop.load(input);

                //get the property value and print it out
                OptionCalculationUtil.setJedis(prop.getProperty("jedis.url"));
                SAMPLE_APPLICATION_NAME = prop.getProperty("kinesis.appName");
                SAMPLE_APPLICATION_STREAM_NAME = prop.getProperty("kinesis.streamName");




            }

        } catch (IOException ex) {
            ex.printStackTrace();
        }

    }



    public static void main(String[] args) throws Exception {
        init();


        if (args.length == 1 && "delete-resources".equals(args[0])) {
            //deleteResources();
            return;
        }

        String workerId = InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID();
        KinesisClientLibConfiguration kinesisClientLibConfiguration =
                new KinesisClientLibConfiguration(SAMPLE_APPLICATION_NAME,
                        SAMPLE_APPLICATION_STREAM_NAME,
                        credentialsProvider,
                        workerId);
        kinesisClientLibConfiguration.withInitialPositionInStream(SAMPLE_APPLICATION_INITIAL_POSITION_IN_STREAM);
        kinesisClientLibConfiguration.withRegionName(Regions.US_EAST_2.getName());

        IRecordProcessorFactory recordProcessorFactory = new KinesisRecordProcessorFactory();
        Worker worker = new Worker(recordProcessorFactory, kinesisClientLibConfiguration);

        System.out.printf("Running %s to process stream %s as worker %s...\n",
                SAMPLE_APPLICATION_NAME,
                SAMPLE_APPLICATION_STREAM_NAME,
                workerId);

        int exitCode = 0;
        try {
            worker.run();
        } catch (Throwable t) {
            System.err.println("Caught throwable while processing data.");
            t.printStackTrace();
            exitCode = 1;
        }
        System.exit(exitCode);
    }




}





