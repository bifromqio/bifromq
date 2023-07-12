# BifroMQ

[English](./README.md) | 中文简体

---


BifroMQ 是一个高性能的分布式 MQTT Broker 消息中间件实现，无缝集成了原生的多租户支持。它旨在支持构建大规模的物联网设备连接和消息系统。

它来源与百度物联网团队多年技术积累，并作为百度智能云[物联网核心套件 IoT Core](https://cloud.baidu.com/product/iot.html)的基础技术，这是一个公有云的 Serverless MQTT 服务。

## 特性

* 完全支持 MQTT 3.1/3.1.1的特性，包括 TCP, TLS, WS, WSS，即将支持 MQTT 5
* 原生支持多租户资源共享和工作负载隔离
* 内置存储引擎，针对关键负载定向优化，无第三方中间件依赖。
* 扩展机制支持：
    * 认证/授权 (Authentication/Authorization)
    * 运行时设置 (Runtime Setting)
    * 桥接 (Bridging)
    * 事件 (Event)
    * 系统/租户级别的监控 (System/Tenant-level Monitoring)

## 开始使用

### 预备条件

* JDK 17+
* Maven 3.5.0+

### 从源码构建

将仓库克隆到您的本地工作空间：

```
cd <YOUR_WORKSPACE>
git clone https://github.com/baidu/bifromq bifromq
```

进入项目文件夹，执行以下命令来构建整个项目：

```
cd bifromq
mvn wrapper:wrapper
./mvnw -U clean package
```

构建输出包括位于`/build/build-bifromq-starters/target/`下：

* `bifromq-<VERSION>-windows-standalone.zip`
* `bifromq-<VERSION>-standalone.tar.gz`

## 运行测试

在项目根文件夹执行以下命令来运行所有测试用例，包括单元测试和集成测试。
注意：测试可能需要一些时间来完成

```
mvn test
```

## 部署

BifroMQ 有三种部署模式：`单机模式(Standalone)`，`标准集群(Standard Cluster)`，`独立工作负载集群(Independent Workload Cluster)`

### 单机模式

单机部署模式非常适合开发阶段，或不需要即时恢复的生产环境。

要启动一个单机的 bifromq 服务器，将 bifromq-xxx-standalone.tar.gz 解压到一个目录。目录结构如下：

```
|-bin
|-conf
|-lib
|-plugins
```

在bin目录下执行以下命令：

启动服务器：

```
./standalone.sh start // 服务进程会在后台运行
```

停止服务器：

```
./standalone.sh stop
```

配置文件 'standalone.yml' 位于 conf 目录中。

大部分设置可以通过名称理解其含义。默认情况下，单机服务器将在`data`目录中保存持久数据。

### 标准集群（即将公开）

标准集群部署模式适用于需要可靠性和可扩展性的小型到中型生产环境。

它由几个完全功能的节点组成，共同作为一个逻辑 MQTT Broker 实例，确保高可用性。 

你也可以通过添加更多的节点来扩大并发 MQTT 连接的工作负载，而在这种模式下，某些类型的消息相关的工作负载并不是水平可扩展的。

### 独立工作负载集群

独立工作负载集群部署模式旨在构建大规模的，多租户的 Serverless 集群。在这种模式下，集群由几个专门的子集群组成，每个子集群都专注于一个特定的'独立类型'的工作负载。这些子集群共同协作形成一个逻辑的 MQTT Broker 实例。这是最复杂的部署模式，需要额外的非开源协作组件。如需商业支持，请随时与我们联系。

## 讨论

如果你对我们的项目感兴趣，你可以微信扫描二维码添加小助手，邀请入群。

<img decoding="async" src="https://bifromq.io/img/qrcode.png" width="30%">
