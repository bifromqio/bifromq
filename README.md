# BifroMQ

English | [中文简体](./README.zh_Hans.md)

---

BifroMQ is a high-performance, distributed MQTT broker implementation that seamlessly integrates native multi-tenancy
support. It is designed to support building large-scale IoT device connections and messaging systems, Currently, it
serves as the foundational technology for Baidu AI Cloud [IoTCore](https://cloud.baidu.com/product/iot.html) , a public serverless
cloud service.

## Features

* Full support for MQTT 3.1, 3.1.1 (MQTT5 support coming soon) features over TCP, TLS, WS, WSS
* Native support for multi-tenancy resource sharing and workload isolation
* Built-in storage engine. Optimized for critical load targeting, no third-party middleware dependencies
* Extension mechanism for supporting:
    * Authentication/Authorization
    * Runtime Setting
    * Bridging
    * Event
    * System/Tenant-level Monitoring


## Documentation

You can view the documentation on the official website [BifroMQ Docs](https://bifromq.io/docs/Readme/) .

And you can contribute to the documentation in the GitHub repository [bifromq-docs](https://github.com/baidu/bifromq-docs).

## Getting Started

### Docker
```
docker run -d --name bifromq -p 1883:1883 bifromq/bifromq:latest
```

### Build from source

#### Prerequisites

* JDK 17+
* Maven 3.5.0+

#### Get source & Build

Clone the repository to your local workspace:

```
cd <YOUR_WORKSPACE>
git clone https://github.com/baidu/bifromq bifromq
```

Navigate to the project root folder and execute the following commands to build the entire project:

```
cd bifromq
mvn wrapper:wrapper
./mvnw -U clean package
```

The build output consists of several archive files located under `/build/build-bifromq-starters/target/`

* `bifromq-<VERSION>-windows-standalone.zip`
* `bifromq-<VERSION>-standalone.tar.gz`

#### Running the tests

Execute the following command in the project root folder to run all test cases, including unit tests and integration
tests.
Note: The tests may take some time to finish

```
mvn test
```

#### Deployment

BifroMQ has three deployment modes: `Standalone`, `Standard Cluster`, `Independent Workload Cluster`

##### Standalone

The standalone deployment mode is ideal for the development stage or production environments that do not require
immediate recovery from downtime.

To start a standalone bifromq server, extract bifromq-xxx-standalone.tar.gz into a directory. The directories
are like:

```
|-bin
|-conf
|-lib
|-plugins
```

Execute the following command in the bin directory:

start server:

```
./standalone.sh start // the server process will start in background
```

stop server:

```
./standalone.sh stop
```

The configuration file, 'standalone.yml', is located in the conf directory. Many of the settings are self-described via
name. By default, the standalone server will save persistent data in `data` directory.

##### Standard Cluster(Will be available publicly soon)

The standard cluster deployment mode is suitable for small to medium-sized production environments that require
reliability and scalability. It comprises several fully functional nodes working together as a logical MQTT broker
instance, ensuring high availability. You can also scale up the concurrent mqtt connection workload by adding more
nodes, while some types of messaging related workload are not horizontal scalable in this mode.

##### Independent Workload Cluster

The Independent Workload Cluster deployment mode is designed for building large-scale, multi-tenant serverless clusters.
In this mode, the cluster consists of several specialized sub-clusters, each focusing on a particular 'independent type'
of workload. These sub-clusters work together coherently to form a logical MQTT broker instance. This is the most
complex deployment mode and requires additional non-open-sourced building blocks. Feel free to contact us for commercial
support.

## Discussion

Join our Discord or WeChat group if you are interested in our work.

### Discord

<a href="https://discord.gg/Pfs3QRadRB"><img src="https://img.shields.io/discord/1115542029531885599?logo=discord&logoColor=white" alt="BifroMQ Discord server" /></a>

### WeChat group

<img decoding="async" src="https://bifromq.io/img/qrcode.png" width="30%"/>