以下Kernel参数会影响mqtt broker所在机器能接受的最大连接数

### 内存

* vm.max_map_count: 限制一个进程可以拥有的VMA(虚拟内存区域)的数量, 可放大到221184

### 最大打开文件数

* nofile: 指单进程的最大打开文件数
* nr_open: 指单个进程可分配的最大文件数，通常默认值为1024*1024=1048576
* file-max: 系统内核一共可以打开的最大值，默认值是185745

### NetFilter调优

通过`sysctl -a | grep conntrack`查看当前的参数，以下几个参数决定了最大连接数:

* net.netfilter.nf_conntrack_buckets: 记录连接条目的hashtable的bucket大小
    * 修改命令：`echo 262144 > /sys/module/nf_conntrack/parameters/hashsize`
* net.netfilter.nf_conntrack_max: hashtable最大的条目数，一般为nf_conntrack_buckets * 4
* net.nf_conntrack_max: 同net.netfilter.nf_conntrack_max
* net.netfilter.nf_conntrack_tcp_timeout_fin_wait: 默认 120s -> 30s
* net.netfilter.nf_conntrack_tcp_timeout_time_wait: 默认 120s -> 30s
* net.netfilter.nf_conntrack_tcp_timeout_close_wait: 默认 60s -> 15s
* net.netfilter.nf_conntrack_tcp_timeout_established: 默认 432000 秒（5天）-> 300s


以下sysctl参数会影响大压力下tcp channel性能表现

### Server端及测试发压端TCP调优

推荐使用centos7环境进行部署及压力测试。

centos6环境需要进行系统参数调优: 
* net.core.wmem_max: 最大的TCP数据发送窗口大小（字节）
  * 修改命令：`echo 'net.core.wmem_max=16777216' >> /etc/sysctl.conf`
* net.core.wmem_default: 默认的TCP数据发送窗口大小（字节）
  * 修改命令：`echo 'net.core.wmem_default=262144' >> /etc/sysctl.conf`
* net.core.rmem_max: 最大的TCP数据接收窗口大小（字节）
  * 修改命令：`echo 'net.core.rmem_max=16777216' >> /etc/sysctl.conf`
* net.core.rmem_default: 默认的TCP数据接收窗口大小（字节）
  * 修改命令：`echo 'net.core.rmem_default=262144' >> /etc/sysctl.conf`
* net.ipv4.tcp_rmem: socket接收缓冲区内存使用的下限  警戒值  上限
  * 修改命令：`echo 'net.ipv4.tcp_rmem= 1024 4096 16777216' >> /etc/sysctl.conf`
* net.ipv4.tcp_rmem: socket发送缓冲区内存使用的下限  警戒值  上限
  * 修改命令：`echo 'net.ipv4.tcp_wmem= 1024 4096 16777216' >> /etc/sysctl.conf`
* net.core.optmem_max: 每个socket所允许的最大缓冲区的大小 (字节)
  * 修改命令：`echo 'net.core.optmem_max = 16777216' >> /etc/sysctl.conf`
* net.core.netdev_max_backlog: 网卡设备将请求放入队列的长度
  * 修改命令：`echo 'net.core.netdev_max_backlog = 16384' >> /etc/sysctl.conf`

修改完配置通过`sysctl -p`并重启服务器生效。


