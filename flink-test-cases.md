# Flink 功能测试用例集合

本文档包含20个全面的Apache Flink测试用例，涵盖流处理、批处理、状态管理、窗口操作、连接器等核心功能。

## 测试环境准备

```bash
# 启动Flink集群
./bin/start-cluster.sh

# 检查集群状态
./bin/flink list

# 设置环境变量
export FLINK_HOME=/path/to/flink
export PATH=$FLINK_HOME/bin:$PATH
```

---

## 测试用例1: 基础流处理 - WordCount

### 功能描述
测试Flink基本的流处理能力，实现实时单词计数

### 测试指令
```bash
# 编译测试程序
mvn clean package -DskipTests

# 提交作业
flink run examples/streaming/WordCount.jar --input file:///tmp/input.txt --output file:///tmp/output.txt
```

### 输入数据
```
# /tmp/input.txt
hello world
hello flink
flink streaming
world peace
```

### 预期输出
```
# /tmp/output.txt
(hello,2)
(world,2)
(flink,2)
(streaming,1)
(peace,1)
```

### 验证命令
```bash
cat /tmp/output.txt
```

---

## 测试用例2: 时间窗口操作 - 滑动窗口

### 功能描述
测试基于时间的滑动窗口聚合操作

### 测试指令
```bash
# Java代码示例
flink run -c com.example.SlidingWindowExample target/flink-tests.jar
```

### 输入数据
```json
{"timestamp": 1640995200000, "value": 10, "key": "A"}
{"timestamp": 1640995205000, "value": 20, "key": "A"}
{"timestamp": 1640995210000, "value": 30, "key": "A"}
{"timestamp": 1640995215000, "value": 40, "key": "A"}
```

### 预期输出
```json
{"key": "A", "window_start": 1640995200000, "window_end": 1640995210000, "sum": 30}
{"key": "A", "window_start": 1640995205000, "window_end": 1640995215000, "sum": 50}
{"key": "A", "window_start": 1640995210000, "window_end": 1640995220000, "sum": 70}
```

### 验证方法
检查窗口聚合结果的正确性和时间边界

---

## 测试用例3: 状态管理 - ValueState

### 功能描述
测试Flink的有状态流处理，使用ValueState存储中间结果

### 测试指令
```bash
flink run -c com.example.StatefulProcessExample target/flink-tests.jar
```

### 输入数据
```
user1,login,1640995200
user1,action,1640995205
user1,logout,1640995300
user2,login,1640995210
```

### 预期输出
```
user1: session_duration=100s
user2: session_active
```

### 验证方法
检查状态是否正确维护用户会话信息

---

## 测试用例4: Kafka连接器 - 消费和生产

### 功能描述
测试Flink与Kafka的集成，实现数据的消费和生产

### 测试指令
```bash
# 启动Kafka
kafka-server-start.sh config/server.properties

# 创建topic
kafka-topics.sh --create --topic test-input --bootstrap-server localhost:9092
kafka-topics.sh --create --topic test-output --bootstrap-server localhost:9092

# 运行Flink作业
flink run -c com.example.KafkaExample target/flink-kafka-test.jar
```

### 输入数据
```bash
# 向Kafka发送数据
kafka-console-producer.sh --topic test-input --bootstrap-server localhost:9092
> {"id": 1, "name": "Alice", "age": 25}
> {"id": 2, "name": "Bob", "age": 30}
```

### 预期输出
```bash
# 从Kafka消费数据
kafka-console-consumer.sh --topic test-output --bootstrap-server localhost:9092 --from-beginning
{"id": 1, "name": "Alice", "age": 25, "processed_time": 1640995200000}
{"id": 2, "name": "Bob", "age": 30, "processed_time": 1640995205000}
```

---

## 测试用例5: 批处理 - 数据集Join操作

### 功能描述
测试Flink批处理中的数据集连接操作

### 测试指令
```bash
flink run -c com.example.BatchJoinExample target/flink-batch-test.jar
```

### 输入数据
```
# users.csv
1,Alice,Engineering
2,Bob,Marketing
3,Charlie,Engineering

# departments.csv
Engineering,100
Marketing,200
Sales,300
```

### 预期输出
```
(Alice,Engineering,100)
(Bob,Marketing,200)
(Charlie,Engineering,100)
```

---

## 测试用例6: CEP复杂事件处理 - 模式检测

### 功能描述
测试Flink CEP库进行复杂事件模式检测

### 测试指令
```bash
flink run -c com.example.CEPExample target/flink-cep-test.jar
```

### 输入数据
```json
{"user": "user1", "action": "login", "timestamp": 1640995200}
{"user": "user1", "action": "purchase", "timestamp": 1640995210}
{"user": "user1", "action": "logout", "timestamp": 1640995300}
```

### 预期输出
```json
{"pattern": "login->purchase->logout", "user": "user1", "duration": 100}
```

---

## 测试用例7: 检查点和故障恢复

### 功能描述
测试Flink的检查点机制和故障恢复能力

### 测试指令
```bash
# 启用检查点的作业
flink run -c com.example.CheckpointExample target/flink-checkpoint-test.jar

# 模拟故障
flink cancel <job-id>

# 从检查点恢复
flink run -s hdfs://checkpoints/checkpoint-123 -c com.example.CheckpointExample target/flink-checkpoint-test.jar
```

### 输入数据
```
连续的数据流...
```

### 预期输出
```
作业从故障点恢复，数据处理继续，无数据丢失
```

---

## 测试用例8: 自定义Source和Sink

### 功能描述
测试自定义数据源和数据汇的实现

### 测试指令
```bash
flink run -c com.example.CustomSourceSinkExample target/flink-custom-test.jar
```

### 输入数据
```java
// 自定义Source生成的数据
CustomEvent(id=1, data="test1", timestamp=1640995200)
CustomEvent(id=2, data="test2", timestamp=1640995205)
```

### 预期输出
```
自定义Sink输出: Processed CustomEvent(id=1, data="test1")
自定义Sink输出: Processed CustomEvent(id=2, data="test2")
```

---

## 测试用例9: Table API和SQL查询

### 功能描述
测试Flink Table API和SQL的查询功能

### 测试指令
```bash
flink run -c com.example.TableAPIExample target/flink-table-test.jar
```

### 输入数据
```sql
CREATE TABLE orders (
    order_id INT,
    product STRING,
    quantity INT,
    price DECIMAL(10,2),
    order_time TIMESTAMP(3)
) WITH (
    'connector' = 'filesystem',
    'path' = 'file:///tmp/orders.csv',
    'format' = 'csv'
);
```

### SQL查询
```sql
SELECT product, SUM(quantity * price) as total_revenue
FROM orders
WHERE order_time > CURRENT_TIMESTAMP - INTERVAL '1' HOUR
GROUP BY product;
```

### 预期输出
```
+----------+---------------+
| product  | total_revenue |
+----------+---------------+
| laptop   |      2999.98  |
| mouse    |        59.97  |
+----------+---------------+
```

---

## 测试用例10: 流批一体化 - Bounded Stream

### 功能描述
测试Flink统一的流批处理模型

### 测试指令
```bash
# 有界流处理
flink run -c com.example.BoundedStreamExample target/flink-unified-test.jar --bounded true

# 无界流处理  
flink run -c com.example.BoundedStreamExample target/flink-unified-test.jar --bounded false
```

### 输入数据
```
有界数据集: [1,2,3,4,5]
无界数据流: 1,2,3,4,5,6,7...
```

### 预期输出
```
有界模式: 处理完成，总计15
无界模式: 持续处理中...
```

---

## 测试用例11: 侧输出流 - Side Output

### 功能描述
测试Flink的侧输出功能，将数据分流到不同的输出

### 测试指令
```bash
flink run -c com.example.SideOutputExample target/flink-side-output-test.jar
```

### 输入数据
```json
{"value": 15, "type": "normal"}
{"value": 95, "type": "high"}
{"value": 5, "type": "low"}
```

### 预期输出
```
主输出流: {"value": 15, "type": "normal"}
高值侧输出: {"value": 95, "type": "high"}  
低值侧输出: {"value": 5, "type": "low"}
```

---

## 测试用例12: 广播状态 - Broadcast State

### 功能描述
测试广播状态的使用，实现配置的动态更新

### 测试指令
```bash
flink run -c com.example.BroadcastStateExample target/flink-broadcast-test.jar
```

### 输入数据
```
配置流: {"rule": "filter", "threshold": 100}
数据流: {"id": 1, "value": 150}
数据流: {"id": 2, "value": 50}
```

### 预期输出
```
应用规则filter(threshold=100): 
- 保留: {"id": 1, "value": 150}
- 过滤: {"id": 2, "value": 50}
```

---

## 测试用例13: 异步I/O操作

### 功能描述
测试异步I/O提高外部系统访问性能

### 测试指令
```bash
flink run -c com.example.AsyncIOExample target/flink-async-test.jar
```

### 输入数据
```json
{"user_id": "user1", "action": "click"}
{"user_id": "user2", "action": "view"}
```

### 预期输出
```json
{"user_id": "user1", "action": "click", "user_info": {"name": "Alice", "age": 25}}
{"user_id": "user2", "action": "view", "user_info": {"name": "Bob", "age": 30}}
```

---

## 测试用例14: 水印和延迟数据处理

### 功能描述
测试水印生成和延迟数据的处理机制

### 测试指令
```bash
flink run -c com.example.WatermarkExample target/flink-watermark-test.jar
```

### 输入数据
```
事件时间: 10:00:00, 数据: A
事件时间: 10:00:05, 数据: B  
事件时间: 09:59:55, 数据: C (延迟数据)
```

### 预期输出
```
窗口[10:00:00-10:00:10]: [A, B]
延迟数据处理: C -> 侧输出流
```

---

## 测试用例15: 自定义分区器

### 功能描述
测试自定义分区逻辑，控制数据分发

### 测试指令
```bash
flink run -c com.example.CustomPartitionerExample target/flink-partition-test.jar
```

### 输入数据
```json
{"key": "A", "value": 1}
{"key": "B", "value": 2}
{"key": "A", "value": 3}
```

### 预期输出
```
分区0: {"key": "A", "value": 1}, {"key": "A", "value": 3}
分区1: {"key": "B", "value": 2}
```

---

## 测试用例16: 流表转换 - Stream to Table

### 功能描述
测试DataStream和Table之间的转换

### 测试指令
```bash
flink run -c com.example.StreamTableConversionExample target/flink-conversion-test.jar
```

### 输入数据
```java
// DataStream
DataStream<Tuple2<String, Integer>> stream = ...;
```

### 转换操作
```java
// Stream转Table
Table table = tableEnv.fromDataStream(stream, "name, count");

// Table转Stream  
DataStream<Row> resultStream = tableEnv.toAppendStream(table, Row.class);
```

### 预期输出
```
转换成功，数据在Stream和Table间无损传递
```

---

## 测试用例17: 多流Join - 双流Join

### 功能描述
测试两个数据流的Join操作

### 测试指令
```bash
flink run -c com.example.StreamJoinExample target/flink-join-test.jar
```

### 输入数据
```
流1: (user1, order123, 10:00:00)
流2: (order123, product_A, 100.0, 10:00:01)
流1: (user2, order456, 10:00:05)  
流2: (order456, product_B, 200.0, 10:00:06)
```

### 预期输出
```
Join结果: (user1, order123, product_A, 100.0)
Join结果: (user2, order456, product_B, 200.0)
```

---

## 测试用例18: 资源配置和性能调优

### 功能描述
测试不同资源配置下的作业性能

### 测试指令
```bash
# 配置1: 低资源
flink run -p 2 -tm 1024m -c com.example.PerformanceTest target/flink-perf-test.jar

# 配置2: 高资源
flink run -p 8 -tm 4096m -c com.example.PerformanceTest target/flink-perf-test.jar
```

### 输入数据
```
高吞吐量数据流: 10000 records/second
```

### 预期输出
```
配置1: 延迟=100ms, 吞吐量=5000 records/s
配置2: 延迟=20ms, 吞吐量=15000 records/s
```

### 监控指标
```bash
# 查看作业指标
curl http://localhost:8081/jobs/<job-id>/metrics
```

---

## 测试用例19: 容器化部署 - Docker/Kubernetes

### 功能描述
测试Flink在容器环境中的部署和运行

### 测试指令
```bash
# Docker部署
docker run -p 8081:8081 flink:latest

# Kubernetes部署
kubectl apply -f flink-deployment.yaml

# 提交作业到K8s集群
flink run-application --target kubernetes-application -Dkubernetes.cluster-id=my-flink-cluster target/flink-k8s-test.jar
```

### 配置文件
```yaml
# flink-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: flink-jobmanager
spec:
  replicas: 1
  selector:
    matchLabels:
      app: flink-jobmanager
  template:
    metadata:
      labels:
        app: flink-jobmanager
    spec:
      containers:
      - name: jobmanager
        image: flink:latest
        ports:
        - containerPort: 8081
```

### 预期输出
```
Pod状态: Running
作业状态: RUNNING
Web UI可访问: http://cluster-ip:8081
```

---

## 测试用例20: 端到端集成测试 - 完整数据管道

### 功能描述
测试完整的数据处理管道，从数据摄入到结果输出

### 测试指令
```bash
# 启动完整管道
./start-pipeline.sh

# 监控管道状态
./monitor-pipeline.sh
```

### 数据流架构
```
Kafka -> Flink -> ElasticSearch
  |        |           |
数据源 -> 流处理 -> 结果存储
```

### 输入数据
```json
// Kafka中的原始事件
{"user_id": "user123", "event_type": "purchase", "product_id": "prod456", "amount": 99.99, "timestamp": "2024-01-01T10:00:00Z"}
```

### 处理逻辑
```java
// Flink处理逻辑
events
  .keyBy(event -> event.user_id)
  .window(TumblingEventTimeWindows.of(Time.hours(1)))
  .aggregate(new PurchaseAggregator())
  .addSink(new ElasticsearchSink<>());
```

### 预期输出
```json
// ElasticSearch中的聚合结果
{
  "user_id": "user123",
  "window_start": "2024-01-01T10:00:00Z",
  "window_end": "2024-01-01T11:00:00Z", 
  "total_purchases": 3,
  "total_amount": 299.97,
  "avg_amount": 99.99
}
```

### 验证方法
```bash
# 检查ElasticSearch结果
curl -X GET "localhost:9200/flink-results/_search?pretty"

# 验证数据完整性
./validate-pipeline-results.sh
```

---

## 测试执行总结

### 执行所有测试用例
```bash
#!/bin/bash
# run-all-tests.sh

echo "开始执行Flink功能测试..."

# 设置环境
source ./setup-test-env.sh

# 依次执行所有测试用例
for i in {1..20}; do
    echo "执行测试用例 $i..."
    ./run-test-case-$i.sh
    if [ $? -eq 0 ]; then
        echo "测试用例 $i: 通过 ✓"
    else
        echo "测试用例 $i: 失败 ✗"
    fi
    echo "------------------------"
done

echo "测试执行完成！"
```

### 测试结果验证
```bash
# 生成测试报告
./generate-test-report.sh

# 清理测试环境
./cleanup-test-env.sh
```

这20个测试用例全面覆盖了Flink的核心功能，包括：
- 基础流处理和批处理
- 窗口操作和时间语义
- 状态管理和容错机制
- 连接器集成(Kafka, ElasticSearch等)
- 高级特性(CEP, 异步I/O, 广播状态等)
- 部署和运维(容器化, 性能调优等)

每个测试用例都包含了具体的执行指令、输入输出数据和验证方法，可以直接用于验证Flink集群的功能完整性。