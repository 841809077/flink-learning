package com.flink.learning;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @author liuyongzhi
 * @date 2024-07-24
 */
public class SocketWindowWordCount {

    public static void main(String[] args) throws Exception {
        // 创建Flink任务运行的环境，实时数据流
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // DataStream 是 Flink 中做流处理的核心 API
        // 使用换行符来分割从 socket 流中接收到的文本数据，每当它读取到一个换行符，就会将前面的文本作为一个单独的记录（字符串）
        DataStream<String> text = env.socketTextStream("39.105.15.196", 9002, "\n");
        DataStream<Tuple2<String, Integer>> wordCounts = text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        // 根据空格截取字符串
                        for (String word : s.split("\\s")) {
                            collector.collect(new Tuple2<>(word, 1));
                        }
                    }
                }).keyBy(new KeySelector<Tuple2<String, Integer>, Object>() {
                    @Override
                    public Object getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.f0;
                    }
                }).window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                // .sum(1)是一个转换操作，用于在一个keyed stream上进行聚合操作。这里的1是参数，表示在Tuple2<String, Integer>中要进行求和操作的字段索引，
                // 由于Tuple是从0开始索引的，0表示第一个字段（这里是单词），1表示第二个字段（这里是整数计数）。
                .sum(1);

        // 将结果打印到控制台，如果需要有序，需要设置 parallelism 为 1
//        wordCounts.print().setParallelism(1);
        wordCounts.addSink(new WordCountSink()).name("WordCount log Sink");
        // 重要
        env.execute("Socket Window WordCount");
    }

}
