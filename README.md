# BifroMQ

BifroMQ is a high-performance distributed MQTT broker implementation that integrates native multi-tenancy support. It
is designed to support building large-scale IoT device connections and messaging systems, and now it's the underlying
technology of [Baidu IoTCore](https://cloud.baidu.com/product/iot.html), a public serverless cloud service.

# Features

* 100% Supports MQTT 3.1, 3.1.1 (MQTT5 will be coming soon) features over TCP, TLS, WS, WSS
* Native support for multi-tenancy resource sharing and workload isolation
* Innovative Subscription Trie distribution scheme and efficient parallel topic matching algorithm
* Built-in distributed sharded persistent storage engine optimized for message performance, reliability and scalability
* Extension mechanism for supporting:
    * Authentication/Authorization
    * Runtime Setting
    * Bridging
    * Event
    * System/Tenant-level Monitoring

## Getting Started

### Prerequisites

* JDK 17+
* Maven 3.5.0+

### Build from source

Clone the repository to local workspace

```
cd <YOUR_WORKSPACE>
git clone https://github.com/baidu/bifromq bifromq
```

Change directory to project root folder and execute following commands to build the whole project

```
cd bifromq
mvn wrapper:wrapper
./mvnw -U clean package
```

The build output are two tar.gz files under `/build/build-bifromq-starters/target/`

* bifromq-xxx-all.tar.gz
* bifromq-xxx-standalone.tar.gz

## Running the tests

Run following command under project root folder to run all test cases including unit tests and integration tests.
Note: The tests may take some time to finish

```
mvn test
```

## Deployment

BifroMQ has three deployment modes: `Standalone`, `Standard Cluster`, `Independent Workload Cluster`

### Standalone

The standalone deployment mode is suitable for the development stage or the production environment that does not require
second-level recovery from downtime.

To start a standalone bifromq server, extract bifromq-xxx-standalone.tar.gz into a directory. The directories
are like:

```
|-bin
|-conf
|-lib
|-plugins
```

execute following command in bin directory:

start server:

```
./standalone.sh start [-d] // -d for daemon
```

stop server:

```
./standalone.sh stop
```

The configuration file named 'standalone.yml' is located in conf directory. Many of the settings are self-described via
name. By default, the standalone server will save persistent data in `data` directory.

### Standard Cluster(Will be available publicly soon)

The standard cluster deployment mode is suitable for the small or middle sized production environment that requires
reliability and scalability. It's composed of several full functional nodes working together as a logical mqtt broker
instance and achieving high availability. You can also scale up the concurrent mqtt connection workload by adding more
nodes,
while some types of messaging related workload are not horizontal scalable in this mode.

## Independent Workload Cluster

The Independent Workload Cluster deployment mode is suitable for building large scale multi-tenant serverless cluster.
In this mode, the cluster is composed of several specialized sub-clusters, each sub-cluster itself is focusing on a
particular 'independent type' of workload, and they work together coherently to form a logical mqtt broker instance.
It is the most complex deployment mode and requires additional NON open-sourced building blocks. Feel free to
contact us for commercial support.

## Discussion