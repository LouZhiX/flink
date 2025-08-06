package com.example;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * 测试用例2: 滑动窗口示例
 * 功能: 测试基于时间的滑动窗口聚合操作
 */
public class SlidingWindowExample {
    
    public static void main(String[] args) throws Exception {
        
        // 设置流处理环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        // 创建测试数据源
        DataStream<String> inputStream = env.fromElements(
            "{\"timestamp\": 1640995200000, \"value\": 10, \"key\": \"A\"}",
            "{\"timestamp\": 1640995205000, \"value\": 20, \"key\": \"A\"}",
            "{\"timestamp\": 1640995210000, \"value\": 30, \"key\": \"A\"}",
            "{\"timestamp\": 1640995215000, \"value\": 40, \"key\": \"A\"}",
            "{\"timestamp\": 1640995220000, \"value\": 50, \"key\": \"A\"}"
        );
        
        // 解析JSON并提取时间戳
        DataStream<Tuple3<String, Integer, Long>> parsedStream = inputStream
            .map(new JsonParseFunction())
            .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple3<String, Integer, Long>>() {
                @Override
                public long extractAscendingTimestamp(Tuple3<String, Integer, Long> element) {
                    return element.f2; // 使用timestamp字段作为事件时间
                }
            });
        
        // 应用滑动窗口 (窗口大小10秒，滑动间隔5秒)
        DataStream<String> windowedStream = parsedStream
            .keyBy(value -> value.f0) // 按key分组
            .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
            .apply(new SlidingWindowFunction());
        
        // 输出结果
        windowedStream.print("滑动窗口结果");
        
        // 执行作业
        env.execute("滑动窗口测试");
    }
    
    /**
     * JSON解析函数
     */
    public static class JsonParseFunction implements MapFunction<String, Tuple3<String, Integer, Long>> {
        private final ObjectMapper objectMapper = new ObjectMapper();
        
        @Override
        public Tuple3<String, Integer, Long> map(String jsonString) throws Exception {
            JsonNode jsonNode = objectMapper.readTree(jsonString);
            String key = jsonNode.get("key").asText();
            Integer value = jsonNode.get("value").asInt();
            Long timestamp = jsonNode.get("timestamp").asLong();
            return new Tuple3<>(key, value, timestamp);
        }
    }
    
    /**
     * 滑动窗口聚合函数
     */
    public static class SlidingWindowFunction implements WindowFunction<Tuple3<String, Integer, Long>, String, String, TimeWindow> {
        @Override
        public void apply(String key, TimeWindow window, Iterable<Tuple3<String, Integer, Long>> input, Collector<String> out) {
            int sum = 0;
            int count = 0;
            
            for (Tuple3<String, Integer, Long> element : input) {
                sum += element.f1;
                count++;
            }
            
            String result = String.format(
                "{\"key\": \"%s\", \"window_start\": %d, \"window_end\": %d, \"sum\": %d, \"count\": %d}",
                key, window.getStart(), window.getEnd(), sum, count
            );
            
            out.collect(result);
        }
    }
}