# Flink 流处理示例用例详解

本文档详细介绍Flink streaming examples目录下的所有示例用例，包括具体的执行命令、输入输出格式和测试方法。

## 📋 示例用例总览

| 示例名称 | 功能描述 | 难度 | 主要特性 |
|----------|----------|------|----------|
| WordCount.jar | 实时单词计数 | ⭐ | 基础流处理 |
| SocketWindowWordCount.jar | Socket流窗口计数 | ⭐⭐ | 窗口操作 |
| SessionWindowing.jar | 会话窗口处理 | ⭐⭐⭐ | 会话窗口 |
| StateMachineExample.jar | 状态机示例 | ⭐⭐⭐ | 状态管理 |
| TopSpeedWindowing.jar | 最高速度窗口 | ⭐⭐ | 聚合窗口 |
| WindowJoin.jar | 窗口连接 | ⭐⭐⭐ | 双流Join |
| Iteration.jar | 迭代处理 | ⭐⭐⭐⭐ | 迭代算法 |

---

## 1. WordCount.jar - 实时单词计数

### 🎯 功能描述
实时处理文本流，统计每个单词的出现次数，是最基础的流处理示例。

### 📋 执行命令
```bash
# 基本执行
flink run -m yarn-cluster WordCount.jar

# 指定输入输出
flink run -m yarn-cluster WordCount.jar \
  --input file:///tmp/input.txt \
  --output file:///tmp/wordcount-output
```

### 📥 输入数据示例
```text
# /tmp/input.txt
hello world flink streaming
apache flink is great
streaming data processing
real time analytics
flink wordcount example
hello flink hello world
```

### 📤 预期输出
```text
# /tmp/wordcount-output
(hello,3)
(world,2)
(flink,4)
(streaming,2)
(apache,1)
(is,1)
(great,1)
(data,1)
(processing,1)
(real,1)
(time,1)
(analytics,1)
(wordcount,1)
(example,1)
```

### 🔧 测试脚本
```bash
#!/bin/bash
# test-wordcount.sh

echo "=== WordCount 流处理测试 ==="

# 准备测试数据
cat > /tmp/wordcount-input.txt << 'EOF'
hello world flink streaming
apache flink is great
streaming data processing
real time analytics
flink wordcount example
hello flink hello world
EOF

# 清理输出
rm -rf /tmp/wordcount-output

# 运行WordCount
flink run WordCount.jar \
  --input file:///tmp/wordcount-input.txt \
  --output file:///tmp/wordcount-output

# 查看结果
echo "WordCount结果:"
cat /tmp/wordcount-output
```

---

## 2. SocketWindowWordCount.jar - Socket流窗口计数

### 🎯 功能描述
从Socket连接读取数据流，在时间窗口内统计单词出现次数。

### 📋 执行命令
```bash
# 默认连接localhost:9999
flink run -m yarn-cluster SocketWindowWordCount.jar

# 指定主机和端口
flink run -m yarn-cluster SocketWindowWordCount.jar \
  --hostname localhost \
  --port 9999
```

### 📥 输入数据准备
```bash
# 启动Socket服务器 (在另一个终端)
nc -lk 9999

# 或使用Python启动Socket服务器
python3 -c "
import socket
import time
import threading

def socket_server():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(('localhost', 9999))
    server.listen(1)
    print('Socket server listening on port 9999')
    
    while True:
        client, addr = server.accept()
        print(f'Client connected: {addr}')
        
        # 发送测试数据
        test_data = [
            'hello world\\n',
            'flink streaming\\n',
            'window wordcount\\n',
            'hello flink\\n',
            'streaming data\\n'
        ]
        
        for data in test_data:
            client.send(data.encode())
            time.sleep(2)
        
        client.close()

socket_server()
"
```

### 📥 输入数据流
```text
# 通过Socket发送的数据
hello world
flink streaming  
window wordcount
hello flink
streaming data
real time processing
hello world again
```

### 📤 预期输出
```text
# 每5秒一个窗口的输出
Window: [2024-01-15 10:00:00, 2024-01-15 10:00:05)
hello : 2
world : 1
flink : 1
streaming : 1

Window: [2024-01-15 10:00:05, 2024-01-15 10:00:10)
window : 1
wordcount : 1
flink : 1
streaming : 1
data : 1

Window: [2024-01-15 10:00:10, 2024-01-15 10:00:15)
real : 1
time : 1
processing : 1
hello : 1
world : 1
again : 1
```

### 🔧 完整测试脚本
```bash
#!/bin/bash
# test-socket-wordcount.sh

echo "=== Socket Window WordCount 测试 ==="

# 启动Socket数据发送器 (后台运行)
python3 -c "
import socket
import time
import sys

def send_data():
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(('localhost', 9999))
        
        test_data = [
            'hello world',
            'flink streaming',
            'window wordcount',
            'hello flink',
            'streaming data',
            'real time processing',
            'hello world again'
        ]
        
        for data in test_data:
            sock.send((data + '\n').encode())
            print(f'Sent: {data}')
            time.sleep(3)
        
        sock.close()
    except Exception as e:
        print(f'Error: {e}')

send_data()
" &

SENDER_PID=$!

# 等待一下再启动Flink作业
sleep 2

# 运行SocketWindowWordCount
flink run SocketWindowWordCount.jar \
  --hostname localhost \
  --port 9999

# 清理后台进程
kill $SENDER_PID 2>/dev/null
```

---

## 3. SessionWindowing.jar - 会话窗口处理

### 🎯 功能描述
基于用户会话的窗口处理，当用户在一定时间内没有活动时，会话窗口关闭。

### 📋 执行命令
```bash
# 默认会话超时30秒
flink run -m yarn-cluster SessionWindowing.jar

# 自定义会话超时时间
flink run -m yarn-cluster SessionWindowing.jar \
  --session-timeout 60000
```

### 📥 输入数据格式
```json
# 用户活动事件流 (JSON格式)
{"userId": "user1", "action": "login", "timestamp": 1642234567000}
{"userId": "user1", "action": "click", "timestamp": 1642234570000}
{"userId": "user1", "action": "view", "timestamp": 1642234575000}
{"userId": "user2", "action": "login", "timestamp": 1642234580000}
{"userId": "user1", "action": "purchase", "timestamp": 1642234590000}
{"userId": "user2", "action": "logout", "timestamp": 1642234620000}
{"userId": "user1", "action": "logout", "timestamp": 1642234650000}
```

### 📤 预期输出
```text
# 会话窗口结果
Session Window [user1]: 
  Start: 2024-01-15 10:00:00
  End: 2024-01-15 10:01:30
  Duration: 90 seconds
  Events: [login, click, view, purchase, logout]
  Event Count: 5

Session Window [user2]:
  Start: 2024-01-15 10:00:20
  End: 2024-01-15 10:01:00
  Duration: 40 seconds
  Events: [login, logout]
  Event Count: 2
```

### 🔧 测试脚本
```bash
#!/bin/bash
# test-session-windowing.sh

echo "=== Session Windowing 测试 ==="

# 创建测试数据
cat > /tmp/session-events.json << 'EOF'
{"userId": "user1", "action": "login", "timestamp": 1642234567000}
{"userId": "user1", "action": "click", "timestamp": 1642234570000}
{"userId": "user1", "action": "view", "timestamp": 1642234575000}
{"userId": "user2", "action": "login", "timestamp": 1642234580000}
{"userId": "user1", "action": "purchase", "timestamp": 1642234590000}
{"userId": "user2", "action": "logout", "timestamp": 1642234620000}
{"userId": "user1", "action": "logout", "timestamp": 1642234650000}
EOF

# 运行会话窗口处理
flink run SessionWindowing.jar \
  --input file:///tmp/session-events.json \
  --output file:///tmp/session-output \
  --session-timeout 30000

echo "会话窗口结果:"
cat /tmp/session-output
```

---

## 4. StateMachineExample.jar - 状态机示例

### 🎯 功能描述
演示复杂事件处理中的状态机模式，跟踪事件序列的状态转换。

### 📋 执行命令
```bash
# 基本执行
flink run -m yarn-cluster StateMachineExample.jar

# 指定状态机配置
flink run -m yarn-cluster StateMachineExample.jar \
  --error-rate 0.1 \
  --info-rate 0.5
```

### 📥 输入数据格式
```json
# 状态事件流
{"sourceAddress": "192.168.1.1", "targetAddress": "10.0.0.1", "eventType": "WARNING", "timestamp": 1642234567000}
{"sourceAddress": "192.168.1.1", "targetAddress": "10.0.0.1", "eventType": "CRITICAL", "timestamp": 1642234570000}
{"sourceAddress": "192.168.1.2", "targetAddress": "10.0.0.2", "eventType": "INFO", "timestamp": 1642234575000}
{"sourceAddress": "192.168.1.1", "targetAddress": "10.0.0.1", "eventType": "CRITICAL", "timestamp": 1642234580000}
{"sourceAddress": "192.168.1.3", "targetAddress": "10.0.0.3", "eventType": "WARNING", "timestamp": 1642234585000}
```

### 📤 预期输出
```text
# 状态机输出
State Machine Alert:
  Source: 192.168.1.1 -> Target: 10.0.0.1
  Pattern: WARNING -> CRITICAL -> CRITICAL
  State: CRITICAL_STATE
  Alert Level: HIGH
  Duration: 13 seconds

State Machine Transition:
  Source: 192.168.1.2 -> Target: 10.0.0.2  
  State: NORMAL -> INFO_STATE
  Event: INFO
  Timestamp: 2024-01-15 10:00:15
```

### 🔧 测试脚本
```bash
#!/bin/bash
# test-state-machine.sh

echo "=== State Machine Example 测试 ==="

# 创建状态事件数据
cat > /tmp/state-events.json << 'EOF'
{"sourceAddress": "192.168.1.1", "targetAddress": "10.0.0.1", "eventType": "WARNING", "timestamp": 1642234567000}
{"sourceAddress": "192.168.1.1", "targetAddress": "10.0.0.1", "eventType": "CRITICAL", "timestamp": 1642234570000}
{"sourceAddress": "192.168.1.2", "targetAddress": "10.0.0.2", "eventType": "INFO", "timestamp": 1642234575000}
{"sourceAddress": "192.168.1.1", "targetAddress": "10.0.0.1", "eventType": "CRITICAL", "timestamp": 1642234580000}
{"sourceAddress": "192.168.1.3", "targetAddress": "10.0.0.3", "eventType": "WARNING", "timestamp": 1642234585000}
{"sourceAddress": "192.168.1.3", "targetAddress": "10.0.0.3", "eventType": "CRITICAL", "timestamp": 1642234590000}
EOF

# 运行状态机示例
flink run StateMachineExample.jar \
  --input file:///tmp/state-events.json \
  --output file:///tmp/state-machine-output

echo "状态机结果:"
cat /tmp/state-machine-output
```

---

## 5. TopSpeedWindowing.jar - 最高速度窗口

### 🎯 功能描述
在时间窗口内计算车辆的最高行驶速度，演示聚合窗口操作。

### 📋 执行命令
```bash
# 基本执行
flink run -m yarn-cluster TopSpeedWindowing.jar

# 指定窗口大小
flink run -m yarn-cluster TopSpeedWindowing.jar \
  --window-size 10000
```

### 📥 输入数据格式
```json
# 车辆速度数据流
{"carId": "car1", "speed": 65, "timestamp": 1642234567000}
{"carId": "car2", "speed": 72, "timestamp": 1642234568000}
{"carId": "car1", "speed": 68, "timestamp": 1642234570000}
{"carId": "car3", "speed": 85, "timestamp": 1642234572000}
{"carId": "car2", "speed": 79, "timestamp": 1642234575000}
{"carId": "car1", "speed": 71, "timestamp": 1642234578000}
{"carId": "car3", "speed": 92, "timestamp": 1642234580000}
```

### 📤 预期输出
```text
# 最高速度窗口结果
Top Speed Window [2024-01-15 10:00:00 - 2024-01-15 10:00:10]:
  car1: 71 km/h
  car2: 79 km/h  
  car3: 92 km/h
  Overall Max: 92 km/h (car3)

Top Speed Window [2024-01-15 10:00:10 - 2024-01-15 10:00:20]:
  car1: 75 km/h
  car2: 83 km/h
  car3: 88 km/h
  Overall Max: 88 km/h (car3)
```

### 🔧 测试脚本
```bash
#!/bin/bash
# test-top-speed.sh

echo "=== Top Speed Windowing 测试 ==="

# 创建车辆速度数据
cat > /tmp/car-speed-data.json << 'EOF'
{"carId": "car1", "speed": 65, "timestamp": 1642234567000}
{"carId": "car2", "speed": 72, "timestamp": 1642234568000}
{"carId": "car1", "speed": 68, "timestamp": 1642234570000}
{"carId": "car3", "speed": 85, "timestamp": 1642234572000}
{"carId": "car2", "speed": 79, "timestamp": 1642234575000}
{"carId": "car1", "speed": 71, "timestamp": 1642234578000}
{"carId": "car3", "speed": 92, "timestamp": 1642234580000}
{"carId": "car1", "speed": 75, "timestamp": 1642234585000}
{"carId": "car2", "speed": 83, "timestamp": 1642234588000}
{"carId": "car3", "speed": 88, "timestamp": 1642234590000}
EOF

# 运行最高速度窗口
flink run TopSpeedWindowing.jar \
  --input file:///tmp/car-speed-data.json \
  --output file:///tmp/top-speed-output \
  --window-size 10000

echo "最高速度结果:"
cat /tmp/top-speed-output
```

---

## 6. WindowJoin.jar - 窗口连接

### 🎯 功能描述
在时间窗口内连接两个数据流，演示双流Join操作。

### 📋 执行命令
```bash
# 基本执行
flink run -m yarn-cluster WindowJoin.jar

# 指定窗口参数
flink run -m yarn-cluster WindowJoin.jar \
  --window-size 5000 \
  --slide-size 1000
```

### 📥 输入数据格式

#### 流1: 用户点击事件
```json
{"userId": "user1", "action": "click", "pageId": "page1", "timestamp": 1642234567000}
{"userId": "user2", "action": "click", "pageId": "page2", "timestamp": 1642234570000}
{"userId": "user1", "action": "click", "pageId": "page3", "timestamp": 1642234575000}
```

#### 流2: 用户信息
```json
{"userId": "user1", "name": "Alice", "age": 25, "timestamp": 1642234568000}
{"userId": "user2", "name": "Bob", "age": 30, "timestamp": 1642234572000}
{"userId": "user3", "name": "Charlie", "age": 35, "timestamp": 1642234578000}
```

### 📤 预期输出
```json
# 窗口Join结果
{
  "window": "[2024-01-15 10:00:00, 2024-01-15 10:00:05)",
  "joinResult": {
    "userId": "user1",
    "action": "click",
    "pageId": "page1", 
    "userName": "Alice",
    "userAge": 25
  }
}
{
  "window": "[2024-01-15 10:00:05, 2024-01-15 10:00:10)",
  "joinResult": {
    "userId": "user2",
    "action": "click", 
    "pageId": "page2",
    "userName": "Bob",
    "userAge": 30
  }
}
```

### 🔧 测试脚本
```bash
#!/bin/bash
# test-window-join.sh

echo "=== Window Join 测试 ==="

# 创建用户点击事件数据
cat > /tmp/click-events.json << 'EOF'
{"userId": "user1", "action": "click", "pageId": "page1", "timestamp": 1642234567000}
{"userId": "user2", "action": "click", "pageId": "page2", "timestamp": 1642234570000}
{"userId": "user1", "action": "click", "pageId": "page3", "timestamp": 1642234575000}
{"userId": "user3", "action": "click", "pageId": "page4", "timestamp": 1642234580000}
EOF

# 创建用户信息数据
cat > /tmp/user-info.json << 'EOF'
{"userId": "user1", "name": "Alice", "age": 25, "timestamp": 1642234568000}
{"userId": "user2", "name": "Bob", "age": 30, "timestamp": 1642234572000}
{"userId": "user3", "name": "Charlie", "age": 35, "timestamp": 1642234578000}
EOF

# 运行窗口Join
flink run WindowJoin.jar \
  --input1 file:///tmp/click-events.json \
  --input2 file:///tmp/user-info.json \
  --output file:///tmp/window-join-output \
  --window-size 5000

echo "窗口Join结果:"
cat /tmp/window-join-output
```

---

## 7. Iteration.jar - 迭代处理

### 🎯 功能描述
演示流处理中的迭代算法，如图算法中的迭代计算。

### 📋 执行命令
```bash
# 基本执行
flink run -m yarn-cluster Iteration.jar

# 指定迭代参数
flink run -m yarn-cluster Iteration.jar \
  --iterations 10 \
  --parallelism 4
```

### 📥 输入数据格式
```json
# 图节点数据
{"nodeId": 1, "value": 1.0, "neighbors": [2, 3]}
{"nodeId": 2, "value": 0.5, "neighbors": [1, 3, 4]}
{"nodeId": 3, "value": 0.8, "neighbors": [1, 2, 4]}
{"nodeId": 4, "value": 0.3, "neighbors": [2, 3]}
```

### 📤 预期输出
```text
# 迭代计算结果
Iteration 1:
  Node 1: value = 0.75
  Node 2: value = 0.65  
  Node 3: value = 0.70
  Node 4: value = 0.55

Iteration 2:
  Node 1: value = 0.68
  Node 2: value = 0.62
  Node 3: value = 0.66
  Node 4: value = 0.58

...

Final Result (Iteration 10):
  Node 1: value = 0.625 (converged)
  Node 2: value = 0.625 (converged)
  Node 3: value = 0.625 (converged)
  Node 4: value = 0.625 (converged)
```

### 🔧 测试脚本
```bash
#!/bin/bash
# test-iteration.sh

echo "=== Iteration 测试 ==="

# 创建图节点数据
cat > /tmp/graph-nodes.json << 'EOF'
{"nodeId": 1, "value": 1.0, "neighbors": [2, 3]}
{"nodeId": 2, "value": 0.5, "neighbors": [1, 3, 4]}
{"nodeId": 3, "value": 0.8, "neighbors": [1, 2, 4]}
{"nodeId": 4, "value": 0.3, "neighbors": [2, 3]}
{"nodeId": 5, "value": 0.9, "neighbors": [1, 4]}
EOF

# 运行迭代处理
flink run Iteration.jar \
  --input file:///tmp/graph-nodes.json \
  --output file:///tmp/iteration-output \
  --iterations 10 \
  --parallelism 2

echo "迭代处理结果:"
cat /tmp/iteration-output
```

---

## 🚀 批量测试脚本

### 执行所有流处理示例
```bash
#!/bin/bash
# run-all-streaming-examples.sh

set -e

echo "=========================================="
echo "    Flink 流处理示例批量测试"
echo "=========================================="

FLINK_HOME="/usr/local/service/flink"
EXAMPLES_DIR="$FLINK_HOME/examples/streaming"
TEST_RESULTS_DIR="/tmp/flink-streaming-test-results"

# 创建结果目录
mkdir -p $TEST_RESULTS_DIR

# 测试用例列表
declare -a EXAMPLES=(
    "WordCount.jar"
    "SocketWindowWordCount.jar" 
    "SessionWindowing.jar"
    "StateMachineExample.jar"
    "TopSpeedWindowing.jar"
    "WindowJoin.jar"
    "Iteration.jar"
)

# 执行测试
for example in "${EXAMPLES[@]}"; do
    echo ""
    echo "=== 测试 $example ==="
    
    if [ -f "$EXAMPLES_DIR/$example" ]; then
        echo "✓ 找到示例文件: $example"
        
        # 根据不同示例执行不同的测试
        case $example in
            "WordCount.jar")
                ./test-wordcount.sh > "$TEST_RESULTS_DIR/wordcount.log" 2>&1
                ;;
            "SocketWindowWordCount.jar")
                ./test-socket-wordcount.sh > "$TEST_RESULTS_DIR/socket-wordcount.log" 2>&1
                ;;
            "SessionWindowing.jar")
                ./test-session-windowing.sh > "$TEST_RESULTS_DIR/session-windowing.log" 2>&1
                ;;
            "StateMachineExample.jar")
                ./test-state-machine.sh > "$TEST_RESULTS_DIR/state-machine.log" 2>&1
                ;;
            "TopSpeedWindowing.jar")
                ./test-top-speed.sh > "$TEST_RESULTS_DIR/top-speed.log" 2>&1
                ;;
            "WindowJoin.jar")
                ./test-window-join.sh > "$TEST_RESULTS_DIR/window-join.log" 2>&1
                ;;
            "Iteration.jar")
                ./test-iteration.sh > "$TEST_RESULTS_DIR/iteration.log" 2>&1
                ;;
        esac
        
        if [ $? -eq 0 ]; then
            echo "✅ $example 测试成功"
        else
            echo "❌ $example 测试失败"
        fi
    else
        echo "❌ 示例文件不存在: $example"
    fi
done

echo ""
echo "=========================================="
echo "           批量测试完成"
echo "=========================================="
echo "测试结果保存在: $TEST_RESULTS_DIR"
echo "查看详细日志: ls -la $TEST_RESULTS_DIR"
```

## 📊 性能对比

| 示例 | 数据量 | 预期延迟 | 吞吐量 | 资源使用 |
|------|--------|----------|---------|----------|
| WordCount | 1000 words | < 100ms | 10K words/s | 低 |
| SocketWindowWordCount | 连续流 | < 200ms | 5K records/s | 中 |
| SessionWindowing | 100 sessions | < 500ms | 1K events/s | 中 |
| StateMachineExample | 500 events | < 300ms | 2K events/s | 中 |
| TopSpeedWindowing | 1000 records | < 150ms | 8K records/s | 低 |
| WindowJoin | 双流各500 | < 400ms | 3K records/s | 高 |
| Iteration | 100 nodes | < 1000ms | 500 iterations/s | 高 |

## 🔧 常见问题解决

### 1. Socket连接问题
```bash
# 错误: Connection refused
# 解决: 确保Socket服务器已启动
nc -lk 9999 &
```

### 2. 内存不足
```bash
# 增加TaskManager内存
flink run -ytm 2048 -yjm 1024 example.jar
```

### 3. 并行度调整
```bash
# 设置合适的并行度
flink run -p 4 example.jar
```

这些示例涵盖了Flink流处理的核心功能，每个都有详细的输入输出说明和可执行的测试脚本，可以帮助您深入理解Flink的流处理能力！