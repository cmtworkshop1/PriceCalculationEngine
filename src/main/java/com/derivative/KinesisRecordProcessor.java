package com.derivative;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

public class KinesisRecordProcessor implements IRecordProcessor {

    private static final Log LOG = LogFactory.getLog(KinesisRecordProcessor.class);
    private String kinesisShardId;

    // Backoff and retry settings
    private static final long BACKOFF_TIME_IN_MILLIS = 3000L;
    private static final int NUM_RETRIES = 2;

    // Checkpoint about once a minute
    private static final long CHECKPOINT_INTERVAL_MILLIS = 60000L;
    private long nextCheckpointTimeInMillis;



    public void initialize(String shardId) {
        LOG.info("Initializing record processor for shard  @@@@@: " + shardId);
        this.kinesisShardId = shardId;

    }

    public void processRecords(List<Record> records, IRecordProcessorCheckpointer checkpointer) {

        System.out.println("Processing #############" + records.size() + " records from " + kinesisShardId);
        long startTime = System.nanoTime();
        System.out.println("Start time"+startTime);
        // Process records and perform all exception handling.
        processRecordsWithRetries(records);
        long timeTaken = System.nanoTime() - startTime;
        System.out.println("End time:"+timeTaken+" for size"+records.size());

        // Checkpoint once every checkpoint interval.
        if (System.currentTimeMillis() > nextCheckpointTimeInMillis) {
            checkpoint(checkpointer);
            nextCheckpointTimeInMillis = System.currentTimeMillis() + CHECKPOINT_INTERVAL_MILLIS;
        }

    }

    /**
     * Process records performing retries as needed. Skip "poison pill" records.
     *
     * @param records Data records to be processed.
     */
    private void processRecordsWithRetries(List<Record> records) {



        for (Record record : records) {

            boolean processedSuccessfully = false;
            for (int i = 0; i < NUM_RETRIES; i++) {
                try {
                    //
                    // Logic to process record goes here.
                    //
                    processSingleRecord(record);
                    processedSuccessfully = true;
                    break;
                } catch (Throwable t) {
                    LOG.warn("Caught throwable while processing record " + record, t);
                }

                // backoff if we encounter an exception.
                try {
                    Thread.sleep(BACKOFF_TIME_IN_MILLIS);
                } catch (InterruptedException e) {
                    LOG.debug("Interrupted sleep", e);
                }
            }

            if (!processedSuccessfully) {
                LOG.error("Couldn't process record " + record + ". Skipping the record.");
            }
        }



    }




    /**
     * Process a single record.
     *
     * @param record The record to be processed.
     */
    private void processSingleRecord(Record record) {

        ArrayList<String> priceDataList = new ArrayList<String>();
        String strData = new String (record.getData().array());
        strData = strData.replaceAll("|","#");
        String optionIdArray[] = strData.split("#");
        System.out.println("The number of options ids per record : "+optionIdArray.length);
        if(optionIdArray.length ==0){
            return;
        }
        String symbol = optionIdArray[0].split("~")[0];
        if("".equals(symbol)){
            return;
        }
        double interestRate = OptionCalculationUtil.getInterestRate(symbol);
        ArrayList<Double> priceVolList = OptionCalculationUtil.getStockPriceAndVol(symbol);

        for (String optionId : optionIdArray){

            if("".equals(optionId)){
                return;
            }

            calculation(optionId, priceDataList,interestRate,priceVolList.get(0),priceVolList.get(1));

        }

       OptionCalculationUtil.writeToRedis(priceDataList);


    }

    /**
     * Process a single record.
     *
     *
     */
    private void calculation(String optionId, ArrayList<String> priceDataList,double interestRate,
                                     double spotPrice, double volatility) {
        // TODO Add your own record processing logic here

        try {

            String strArray[] = optionId.split("~");
            //System.out.println( strArray[0]+":"+strArray[1]+":"+strArray[2]);
            double calculatedCallPrice = OptionCalculationEngine.calculate(true,
                    spotPrice,
                    Double.valueOf(strArray[2]),
                    interestRate,
                    OptionCalculationUtil.getTimeToExpiry(strArray[1]),
                    volatility);


            priceDataList.add(optionId);
            BigDecimal bigDecimal = new BigDecimal(calculatedCallPrice);
            priceDataList.add(bigDecimal.setScale(2, BigDecimal.ROUND_HALF_UP).toPlainString());



        } catch (Exception e) {
            System.out.println("Exception in Kinesis application :"+ e.toString());
            //e.printStackTrace();
            //System.exit(1);
        }
    }


    public void shutdown(IRecordProcessorCheckpointer checkpointer, ShutdownReason reason) {
       LOG.info("Shutting down record processor for shard: " + kinesisShardId);
        // Important to checkpoint after reaching end of shard, so we can start processing data from child shards.
        if (reason == ShutdownReason.TERMINATE) {
            checkpoint(checkpointer);
        }
    }


    private void checkpoint(IRecordProcessorCheckpointer checkpointer) {
        LOG.info("Checkpointing shard " + kinesisShardId);
        for (int i = 0; i < NUM_RETRIES; i++) {
            try {
                checkpointer.checkpoint();
                break;
            } catch (ShutdownException se) {
                // Ignore checkpoint if the processor instance has been shutdown (fail over).
                LOG.info("Caught shutdown exception, skipping checkpoint.", se);
                break;
            } catch (ThrottlingException e) {
                // Backoff and re-attempt checkpoint upon transient failures
                if (i >= (NUM_RETRIES - 1)) {
                    LOG.error("Checkpoint failed after " + (i + 1) + "attempts.", e);
                    break;
                } else {
                    LOG.info("Transient issue when checkpointing - attempt " + (i + 1) + " of "
                            + NUM_RETRIES, e);
                }
            } catch (InvalidStateException e) {
                // This indicates an issue with the DynamoDB table (check for table, provisioned IOPS).
                LOG.error("Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.", e);
                break;
            }
            try {
                Thread.sleep(BACKOFF_TIME_IN_MILLIS);
            } catch (InterruptedException e) {
                LOG.debug("Interrupted sleep", e);
            }
        }

}




}
