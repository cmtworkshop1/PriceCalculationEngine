package com.derivative;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;

public class KinesisRecordProcessorFactory implements IRecordProcessorFactory {

        public IRecordProcessor createProcessor() {
            return new KinesisRecordProcessor();
        }
}



