# Flink KMeans 聚类算法 - YARN集群运行示例

## 🎯 场景描述
在YARN集群上运行Flink的KMeans聚类算法，使用腾讯云COS作为数据存储。

## 📋 执行命令
```bash
flink/bin/flink run -m yarn-cluster \
  /usr/local/service/flink/examples/batch/KMeans.jar \
  --input cosn://junglelou-1258469122/test.txt \
  --output cosn://junglelou-1258469122/res_kmeans.txt
```

## 📥 输入数据格式

### 输入文件: `cosn://junglelou-1258469122/test.txt`

KMeans算法需要二维坐标点数据，每行一个点，格式为：`x坐标 y坐标`

```text
# 示例数据点 - 形成3个明显的聚类
# 第一个聚类中心附近的点 (约在 (2,2) 周围)
1.0 1.5
2.5 2.0
1.8 2.3
2.2 1.8
1.5 2.5
2.8 2.2
1.9 1.9
2.1 2.4

# 第二个聚类中心附近的点 (约在 (8,8) 周围)
7.5 8.2
8.3 7.8
8.1 8.5
7.8 7.5
8.5 8.1
7.9 8.3
8.2 7.9
8.0 8.0

# 第三个聚类中心附近的点 (约在 (5,1) 周围)
4.8 1.2
5.2 0.8
4.9 1.5
5.1 1.1
4.7 1.3
5.3 0.9
4.6 1.4
5.0 1.0

# 一些噪声点
3.5 4.5
6.2 3.8
1.2 5.1
7.1 2.3
```

### 完整测试数据集 (100个数据点)
```text
# 聚类1: 中心约在 (2, 2)
1.2 1.8
1.8 2.3
2.1 1.9
2.5 2.2
1.9 2.1
2.3 1.7
1.6 2.4
2.0 2.0
1.7 1.6
2.4 2.3
1.5 2.2
2.2 1.8
1.9 2.5
2.1 2.1
1.8 1.9
2.6 2.0
1.4 2.1
2.3 2.4
2.0 1.5
1.7 2.2
2.5 1.9
1.6 1.8
2.2 2.3
1.9 1.7
2.4 2.1
1.8 2.0
2.1 2.2
1.5 1.9
2.3 2.5
2.0 2.4

# 聚类2: 中心约在 (8, 8)
7.8 8.1
8.2 7.9
8.1 8.3
7.9 8.0
8.3 8.2
8.0 7.8
7.7 8.4
8.4 8.1
7.9 7.9
8.1 8.2
8.2 8.0
7.8 8.3
8.0 8.1
8.3 7.8
7.6 8.2
8.5 8.0
8.1 7.7
7.9 8.4
8.2 8.3
8.0 8.0
7.8 7.9
8.4 8.2
8.1 8.1
7.9 8.3
8.3 8.1
8.0 7.9
7.7 8.0
8.2 8.4
8.1 8.0
7.9 8.2

# 聚类3: 中心约在 (5, 1)
4.8 1.2
5.1 0.9
5.0 1.3
4.9 1.1
5.2 1.0
4.7 1.4
5.3 1.2
4.6 0.8
5.1 1.5
4.8 1.1
5.0 0.9
4.9 1.3
5.2 1.1
4.7 1.0
5.1 1.4
4.8 1.2
5.3 0.9
4.6 1.3
5.0 1.1
4.9 1.0
5.1 1.2
4.8 0.8
5.2 1.4
4.7 1.1
5.0 1.3
4.9 0.9
5.3 1.1
4.6 1.2
5.1 1.0
4.8 1.4

# 一些离群点
3.0 4.0
6.5 3.5
1.0 5.0
7.0 2.0
3.5 6.0
6.0 4.5
2.5 5.5
6.8 3.2
3.2 4.8
6.3 3.8
```

## 📤 预期输出结果

### 输出文件: `cosn://junglelou-1258469122/res_kmeans.txt`

KMeans算法会输出聚类中心的坐标，默认k=3个聚类：

```text
# 聚类中心结果 (格式: 聚类ID 中心X坐标 中心Y坐标)
0 2.01 2.08
1 8.02 8.01
2 4.97 1.12
```

### 详细输出说明
- **聚类0**: 中心坐标约为 (2.01, 2.08) - 对应第一个数据集群
- **聚类1**: 中心坐标约为 (8.02, 8.01) - 对应第二个数据集群  
- **聚类2**: 中心坐标约为 (4.97, 1.12) - 对应第三个数据集群

## 🔧 命令参数说明

### 基本参数
- `-m yarn-cluster`: 指定在YARN集群模式下运行
- `--input`: 输入文件路径 (支持HDFS、COS等分布式存储)
- `--output`: 输出文件路径

### 可选参数
```bash
# 完整命令示例 (包含可选参数)
flink/bin/flink run -m yarn-cluster \
  /usr/local/service/flink/examples/batch/KMeans.jar \
  --input cosn://junglelou-1258469122/test.txt \
  --output cosn://junglelou-1258469122/res_kmeans.txt \
  --iterations 20 \
  --k 3 \
  --parallelism 4
```

参数说明：
- `--iterations 20`: 最大迭代次数 (默认10)
- `--k 3`: 聚类数量 (默认2)
- `--parallelism 4`: 并行度 (默认为集群配置)

## 📊 执行过程输出

### 1. 作业提交阶段
```bash
$ flink/bin/flink run -m yarn-cluster /usr/local/service/flink/examples/batch/KMeans.jar --input cosn://junglelou-1258469122/test.txt --output cosn://junglelou-1258469122/res_kmeans.txt

2024-01-15 10:30:15,123 INFO  org.apache.flink.yarn.YarnClusterDescriptor - Cluster specification: ClusterSpecification{masterMemoryMB=1024, taskManagerMemoryMB=1024, numberTaskManagers=2, slotsPerTaskManager=1}
2024-01-15 10:30:16,456 INFO  org.apache.flink.yarn.YarnClusterDescriptor - Submitting application master application_1642234567890_0001
2024-01-15 10:30:18,789 INFO  org.apache.flink.yarn.YarnClusterDescriptor - YARN application has been deployed successfully.
2024-01-15 10:30:18,790 INFO  org.apache.flink.yarn.YarnClusterDescriptor - The Flink YARN client has been started in detached mode.
```

### 2. 作业执行阶段
```bash
2024-01-15 10:30:20,123 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph - Job KMeans Clustering (JobID: 1234567890abcdef) switched from state CREATED to RUNNING.
2024-01-15 10:30:21,456 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph - Source: Read Text File -> Map -> Reduce (1/2) switched from SCHEDULED to RUNNING.
2024-01-15 10:30:22,789 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph - Iteration (Bulk Iteration) -> Sink: Write Text File (1/2) switched from SCHEDULED to RUNNING.
```

### 3. 迭代过程输出
```bash
2024-01-15 10:30:25,123 INFO  org.apache.flink.examples.java.clustering.KMeans - Iteration 1: Average cost = 15.234
2024-01-15 10:30:27,456 INFO  org.apache.flink.examples.java.clustering.KMeans - Iteration 2: Average cost = 8.567
2024-01-15 10:30:29,789 INFO  org.apache.flink.examples.java.clustering.KMeans - Iteration 3: Average cost = 4.123
2024-01-15 10:30:32,123 INFO  org.apache.flink.examples.java.clustering.KMeans - Iteration 4: Average cost = 2.456
2024-01-15 10:30:34,456 INFO  org.apache.flink.examples.java.clustering.KMeans - Iteration 5: Average cost = 1.234
2024-01-15 10:30:36,789 INFO  org.apache.flink.examples.java.clustering.KMeans - Iteration 6: Average cost = 0.789
2024-01-15 10:30:39,123 INFO  org.apache.flink.examples.java.clustering.KMeans - Iteration 7: Average cost = 0.456
2024-01-15 10:30:41,456 INFO  org.apache.flink.examples.java.clustering.KMeans - Convergence reached after 7 iterations.
```

### 4. 作业完成阶段
```bash
2024-01-15 10:30:43,789 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph - Job KMeans Clustering (JobID: 1234567890abcdef) switched from state RUNNING to FINISHED.
2024-01-15 10:30:44,123 INFO  org.apache.flink.yarn.YarnClusterDescriptor - Application application_1642234567890_0001 finished with final status: SUCCEEDED
```

## 📈 性能指标

### 预期执行时间
- **数据量**: 100个数据点
- **预期执行时间**: 30-60秒
- **收敛迭代次数**: 5-10次
- **资源使用**: 2个TaskManager，每个1GB内存

### 性能优化建议
```bash
# 大数据集优化配置
flink/bin/flink run -m yarn-cluster \
  -ytm 2048 \
  -yjm 1024 \
  -yn 4 \
  -ys 2 \
  /usr/local/service/flink/examples/batch/KMeans.jar \
  --input cosn://junglelou-1258469122/large_test.txt \
  --output cosn://junglelou-1258469122/res_kmeans.txt \
  --parallelism 8 \
  --iterations 50 \
  --k 5
```

参数说明：
- `-ytm 2048`: TaskManager内存2GB
- `-yjm 1024`: JobManager内存1GB
- `-yn 4`: TaskManager数量4个
- `-ys 2`: 每个TaskManager的slot数2个

## 🔍 结果验证

### 1. 检查输出文件
```bash
# 检查输出文件是否存在
hadoop fs -ls cosn://junglelou-1258469122/res_kmeans.txt

# 查看输出内容
hadoop fs -cat cosn://junglelou-1258469122/res_kmeans.txt
```

### 2. 结果质量评估
```python
# Python脚本验证聚类质量
import numpy as np
import matplotlib.pyplot as plt

# 读取原始数据
data_points = np.loadtxt('test.txt')

# 读取聚类中心
centers = np.loadtxt('res_kmeans.txt', usecols=(1, 2))

# 可视化结果
plt.figure(figsize=(10, 8))
plt.scatter(data_points[:, 0], data_points[:, 1], c='blue', alpha=0.6, label='Data Points')
plt.scatter(centers[:, 0], centers[:, 1], c='red', s=200, marker='x', label='Cluster Centers')
plt.xlabel('X Coordinate')
plt.ylabel('Y Coordinate')
plt.title('KMeans Clustering Results')
plt.legend()
plt.grid(True)
plt.show()
```

## ⚠️ 常见问题和解决方案

### 1. 内存不足错误
```bash
# 错误信息
Exception in thread "main" java.lang.OutOfMemoryError: Java heap space

# 解决方案
flink/bin/flink run -m yarn-cluster \
  -ytm 4096 \
  -yjm 2048 \
  /usr/local/service/flink/examples/batch/KMeans.jar \
  --input cosn://junglelou-1258469122/test.txt \
  --output cosn://junglelou-1258469122/res_kmeans.txt
```

### 2. COS连接问题
```bash
# 错误信息
java.io.IOException: cosn://junglelou-1258469122/test.txt (No such file or directory)

# 解决方案
# 1. 检查COS配置
hadoop fs -ls cosn://junglelou-1258469122/

# 2. 确保文件存在
hadoop fs -put local_test.txt cosn://junglelou-1258469122/test.txt
```

### 3. YARN资源不足
```bash
# 错误信息
Application application_xxx failed 2 times due to ApplicationMaster for attempt application_xxx was killed

# 解决方案
# 减少资源需求或增加YARN集群资源
flink/bin/flink run -m yarn-cluster \
  -ytm 1024 \
  -yjm 512 \
  -yn 1 \
  /usr/local/service/flink/examples/batch/KMeans.jar \
  --input cosn://junglelou-1258469122/test.txt \
  --output cosn://junglelou-1258469122/res_kmeans.txt
```

## 📋 完整测试脚本

```bash
#!/bin/bash
# kmeans_test.sh - KMeans聚类测试脚本

set -e

echo "=== Flink KMeans 聚类测试 ==="

# 1. 准备测试数据
echo "准备测试数据..."
cat > test_data.txt << 'EOF'
1.2 1.8
1.8 2.3
2.1 1.9
2.5 2.2
7.8 8.1
8.2 7.9
8.1 8.3
7.9 8.0
4.8 1.2
5.1 0.9
5.0 1.3
4.9 1.1
EOF

# 2. 上传数据到COS
echo "上传数据到COS..."
hadoop fs -put test_data.txt cosn://junglelou-1258469122/test.txt

# 3. 运行KMeans算法
echo "运行KMeans算法..."
flink/bin/flink run -m yarn-cluster \
  /usr/local/service/flink/examples/batch/KMeans.jar \
  --input cosn://junglelou-1258469122/test.txt \
  --output cosn://junglelou-1258469122/res_kmeans.txt \
  --iterations 20 \
  --k 3

# 4. 检查结果
echo "检查结果..."
hadoop fs -cat cosn://junglelou-1258469122/res_kmeans.txt

echo "=== 测试完成 ==="
```

这个示例为您提供了完整的KMeans聚类算法在Flink YARN集群上的运行场景，包括输入数据格式、预期输出结果、执行过程和故障排除方法。