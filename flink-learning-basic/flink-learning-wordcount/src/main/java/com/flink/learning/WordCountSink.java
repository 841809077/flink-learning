package com.flink.learning;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author liuyongzhi
 * @date 2024-07-24
 */
public class WordCountSink implements SinkFunction<Tuple2<String, Integer>> {

    private static final Logger logger = LoggerFactory.getLogger(WordCountSink.class);

    @Override
    public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
        logger.info("Word: {}, Windows Count: {}", value.f0, value.f1);
    }
}
