---
name: Bug report
about: Create a report to help us improve
title: ''
labels: ''
assignees: ''

---

⚠️ **Please ensure that the provided information is as detailed and clear as possible. Lack of information may delay the resolution of the issue.**

**Describe the bug**
A clear and concise description of what the bug is.

**BifroMQ**
- Version: [e.g. 3.1.x **We highly recommend you to try the latest version first.**]
- Deployment: [e.g. Standalone | Cluster | Docker | Kubernetes | etc]

**To Reproduce**

Steps to reproduce the behavior, Please include necessary information such as(but not limited to):

*** PUB Client ***:
- MQTT Connection:
   - Clean Session: [true | false]
   - ClientIdentifier: 
   - etc...
- MQTT Pub:
   - Topic: 
   - QoS: [0|1|2]
   - Retain: [true | false]
*** SUB Client ***
- MQTT Connection:
   - Clean Session: [true | false]
   - ClientIdentifier: 
   - etc...
- MQTT Sub:
   - TopicFilter: 
   - QoS: [0|1|2]

**Expected behavior**
A clear and concise description of what you expected to happen.

**Logs**
If applicable, add related logs to help troubleshoot.

**Configurations**
You can copy from the beginning of info.log and paste here.
See also: https://bifromq.io/docs/admin_guide/configuration/configs_print

**OS(please complete the following information):**
 - OS: [e.g. CentOS 8]
 - Kernel Version [e.g. 5.6]
    - Kernel Specific Settings: [e.g. TCP, FD, etc]

**JVM:**
- Version: [e.g. 17]
- Arguments: [e.g. if override any JVM arguments]

**Performance Related**

If your problem is performance-related, please provide as much detailed information as possible according to the list.

- HOST:
  - Cluster node count: [e.g. 1|3]
  - CPU: [e.g. vCPU number]
  - Memory: [e.g. 32G]
- Network:
  - Bandwidth: [e.g. 1Gbps]
  - Latency: [e.g. 1ms]
- Load:
  - PUB count: [e.g. 1000]
  - SUB count: [e.g. 1000]
  - PUB QPS per connection: [e.g. 10msg/s]
  - SUB QPS per connection: [e.g. 10msg/s]
  - Payload size: [e.g. 1KB]

**Additional context**
Add any other context about the problem here.
