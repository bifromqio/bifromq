---
name: Bug report
about: Create a report to help us improve
title: ''
labels: ''
assignees: ''

---

**Describe the bug**
A clear and concise description of what the bug is.

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

**Screenshots**
If applicable, add screenshots to help explain your problem.

**HOST**
- CPU: [e.g. vCPU number]
- Memory: [e.g. 32G]

**OS(please complete the following information):**
 - OS: [e.g. CentOS 8]
 - Kernel Version [e.g. 5.6]
    - Kernel Specific Settings: [e.g. TCP, FD, etc]

**JVM:**
- Version: [e.g. 17]
- Arguments: [e.g. if override any JVM arguments]

**BifroMQ**
- Version:
- Non-Default Configuration:

**Additional context**
Add any other context about the problem here.
