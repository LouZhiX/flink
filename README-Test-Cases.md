# Flink 功能测试用例使用指南

本项目提供了一套完整的Apache Flink功能测试用例，用于验证Flink集群的各项核心功能。

## 📋 测试用例概览

### 已实现的测试用例
- ✅ **测试用例1**: 基础流处理 - WordCount
- ✅ **测试用例2**: 时间窗口操作 - 滑动窗口
- 🔧 **测试用例3-20**: 其他高级功能 (框架已搭建，待完善实现)

### 测试覆盖范围
1. **基础功能**
   - 流处理和批处理
   - 数据源和数据汇
   - 基本转换操作

2. **时间和窗口**
   - 事件时间和处理时间
   - 滚动窗口、滑动窗口、会话窗口
   - 水印和延迟数据处理

3. **状态管理**
   - ValueState、ListState、MapState
   - 检查点和故障恢复
   - 状态后端配置

4. **连接器集成**
   - Kafka连接器
   - 文件系统连接器
   - ElasticSearch连接器

5. **高级特性**
   - CEP复杂事件处理
   - 异步I/O操作
   - 广播状态
   - 侧输出流

6. **Table API & SQL**
   - 流表转换
   - SQL查询
   - 动态表

7. **部署和运维**
   - 容器化部署
   - 资源配置和性能调优
   - 监控和指标

## 🚀 快速开始

### 环境要求
- Java 8+
- Apache Flink 1.17+
- Maven 3.6+
- Docker (可选)
- Kubernetes (可选)

### 设置环境变量
```bash
export FLINK_HOME=/path/to/your/flink
export PATH=$FLINK_HOME/bin:$PATH
```

### 执行所有测试
```bash
cd test-scripts
chmod +x *.sh
./run-all-tests.sh
```

### 执行单个测试用例
```bash
# 测试用例1: WordCount
./run-test-case-1.sh

# 测试用例2: 滑动窗口
cd java-examples
mvn clean package
cd ..
flink run -c com.example.SlidingWindowExample java-examples/target/flink-test-examples-1.0.0.jar
```

## 📁 项目结构

```
workspace/
├── flink-test-cases.md          # 详细测试用例文档
├── test-scripts/                # 测试脚本目录
│   ├── setup-test-env.sh       # 环境设置脚本
│   ├── run-all-tests.sh        # 执行所有测试
│   ├── run-test-case-1.sh      # 测试用例1脚本
│   ├── generate-test-report.sh # 报告生成脚本
│   ├── cleanup-test-env.sh     # 环境清理脚本
│   └── java-examples/          # Java测试代码
│       ├── pom.xml             # Maven配置
│       └── SlidingWindowExample.java  # 滑动窗口示例
└── README-Test-Cases.md        # 本文档
```

## 🧪 测试用例详情

### 测试用例1: 基础流处理 - WordCount
**功能**: 测试Flink基本的流处理能力
**输入**: 文本文件包含多行文字
**输出**: 单词计数结果
**验证**: 检查输出文件中的单词统计是否正确

### 测试用例2: 时间窗口操作 - 滑动窗口
**功能**: 测试基于时间的滑动窗口聚合
**输入**: JSON格式的时间序列数据
**输出**: 窗口聚合结果
**验证**: 检查窗口边界和聚合计算的正确性

### 测试用例3-20: 高级功能测试
这些测试用例涵盖了Flink的高级功能，包括：
- 状态管理和容错
- 连接器集成
- 复杂事件处理
- Table API和SQL
- 部署和运维

## 📊 测试报告

执行测试后会生成详细的测试报告：

### HTML报告
- 位置: `test-results/flink-test-report-YYYYMMDD-HHMMSS.html`
- 包含: 可视化的测试结果、统计图表、详细状态

### 文本报告
- 位置: `test-results/flink-test-summary-YYYYMMDD-HHMMSS.txt`
- 包含: 简洁的测试总结和建议

### 日志文件
- 位置: `test-results/test-results-YYYYMMDD-HHMMSS.log`
- 包含: 详细的执行日志和错误信息

## 🛠️ 自定义测试用例

### 添加新的测试用例
1. 创建测试脚本: `test-scripts/run-test-case-N.sh`
2. 编写Java代码 (如需要): `test-scripts/java-examples/YourExample.java`
3. 更新Maven配置: `test-scripts/java-examples/pom.xml`
4. 修改主测试脚本: `test-scripts/run-all-tests.sh`

### 测试脚本模板
```bash
#!/bin/bash
# 测试用例N: 功能描述

set -e
echo "=== 执行测试用例N: 功能名称 ==="

# 检查前置条件
# 准备测试数据
# 执行Flink作业
# 验证结果
# 输出测试结果

echo "=== 测试用例N完成 ==="
```

## 🔧 故障排除

### 常见问题

**1. Flink集群启动失败**
```bash
# 检查端口是否被占用
netstat -tlnp | grep 8081

# 检查Java版本
java -version

# 查看Flink日志
tail -f $FLINK_HOME/log/flink-*-standalonesession-*.log
```

**2. 测试用例执行失败**
```bash
# 检查测试数据是否存在
ls -la /tmp/flink-test-data/

# 检查Flink作业状态
flink list

# 查看详细错误日志
cat test-results/test-results-*.log
```

**3. Maven编译失败**
```bash
# 清理并重新编译
cd test-scripts/java-examples
mvn clean compile
mvn dependency:resolve
```

### 环境检查清单
- [ ] FLINK_HOME环境变量已设置
- [ ] Flink集群正常启动 (Web UI可访问)
- [ ] Java版本兼容 (Java 8+)
- [ ] Maven可正常使用
- [ ] 测试数据目录可写入
- [ ] 网络连接正常 (如需要外部服务)

## 📈 性能基准

### 基准测试指标
- **吞吐量**: records/second
- **延迟**: 处理延迟时间
- **资源使用**: CPU、内存使用率
- **故障恢复时间**: 检查点恢复耗时

### 基准测试环境
- **硬件**: 推荐4核8GB内存
- **网络**: 千兆网络
- **存储**: SSD存储
- **并发度**: 根据CPU核数调整

## 🤝 贡献指南

### 如何贡献
1. Fork本项目
2. 创建功能分支: `git checkout -b feature/new-test-case`
3. 提交更改: `git commit -am 'Add new test case'`
4. 推送分支: `git push origin feature/new-test-case`
5. 创建Pull Request

### 代码规范
- 使用有意义的变量和函数名
- 添加适当的注释和文档
- 遵循现有的代码风格
- 确保测试用例可重复执行

## 📚 参考资料

- [Apache Flink官方文档](https://flink.apache.org/docs/)
- [Flink DataStream API](https://flink.apache.org/docs/stable/dev/datastream_api.html)
- [Flink Table API & SQL](https://flink.apache.org/docs/stable/dev/table/)
- [Flink CEP](https://flink.apache.org/docs/stable/dev/libs/cep.html)
- [Flink连接器](https://flink.apache.org/docs/stable/dev/connectors/)

## 📄 许可证

本项目遵循Apache 2.0许可证。详情请参阅LICENSE文件。

---

**联系方式**: 如有问题或建议，请提交Issue或Pull Request。