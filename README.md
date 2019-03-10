# 第一届POLARDB数据库性能大赛
### 1 前言

本次比赛参赛队伍是中间件比赛的原班人马，正好借此比赛的机会对IO操作可以有更深的理解。先暴露一波排名：

![image](https://github.com/Qiwc/polardb-race/blob/master/images/1.jpg)

初赛首先使用JAVA，曾一度是JAVA选手的领头羊，这阶段我们对JAVA磁盘IO的操作有了更深的理解，IO一定要先了解Linux磁盘IO的几种机制，再学习一波JAVA NIO才能融汇贯通。初赛接近尾声阶段，我们使用CPP翻译了一波，最后第5名。

复赛继续在CPP基础上，优化了程序的组织架构，最后Range阶段采用经典的生产者消费者模型，系列优化后文介绍。最后虽然只拿到了第7名，但是从成绩上可以看出：2到7名的成绩差距十分小，可以说没抖好是一部分原因。

[题目链接](https://code.aliyun.com/polar_race2018/competition_rules?spm=5176.12281978.0.0.a42a2138nrSQgp&accounttraceid=48328918-9617-4ef6-9de0-0aa4ed096c8c)

------

### 2 赛题关键点分析

- CPP内存2G、JAVA内存3G
- key 8B、value 4KB
- 64线程，每个线程100w次数据。共256G左右
- 正确性检测阶段kill -9模拟进程意外退出，需保持数据持久化
- recover、write、read、range四个阶段之间均会重启DB引擎

------

### 3 初赛总结

test/DemoTest测试程序用

- 必须要有内存占用量非常小的索引：数组型hashmap或者直接快排
- 必须保证随时kill后数据不丢失：每write一个数据就要刷盘一次
- key分布均匀，可根据key的前几个位，分为256个文件存储。且对同一个文件刷盘，要上锁刷盘，原子量方式虽然看起来锁的粒度很小但是实际更慢了，根据FileChannel的write函数可知，这里面是带锁的会自身带有保证同步的机制，所以用原子量的方式刷盘实际上多做了CAS浪费了时间。
- 刷盘方式：key因为很小，所以使用mmap。value比较大而且存储value的文件大小远大于2G，不再适合使用mmap，因此使用DirectBuffer可减少一次堆外到堆内的复制
- 读取value方式：仍要采用DirectBuffer，但是分配堆外内存是比较耗时的，要使用池化的方式使得DirectBuffer实现复用，因此利用ThreadLocal

------

### 4 复赛总结

test/test.cc测试程序用

#### 4.1 write

对于每一组key/value，通过key的前几位字符，将其分在不同的分片中（最终的版本选择了4096个分片，即根据key的前12位划分成log0-log4095）。每个分片容量为1024*16。当容量不够时，程序可将文件扩容（因为recover阶段的数据不是均匀分配的，为了应对过去必须实现扩容）。

其中，key和value会分开储存。写value时会使用mmap做16kb的缓冲区（防止kill-9），写满16kb后直接IO落盘，这个会比写一个就直接落盘一个快一点。写key时则一直使用mmap，只需存放key的值无需存放value在文件中的offset。

valueFiles由64个文件组成，分片大小及规则如下图（DirectIO）：

![image](https://github.com/Qiwc/polardb-race/blob/master/images/2.jpg)

keyFiles由64个文件组成，包括key部分和16kb写缓存部分，分片大小及规则如下图（mmap）：

![image](https://github.com/Qiwc/polardb-race/blob/master/images/3.jpg)

#### 4.2 read

在open时，如果发现存在数据文件则会进入数据恢复阶段，在该阶段使用多线程恢复所有分片信息，同时会把每个分片的key放入SortLog中，目的是对其进行排序和去重。在恢复时，同时还会预读一小部分value数据到缓存中（最终是预读前4个分片）。

随机读的过程中，判断key所属的分片，并且在SortLog中通过二分查找获取key所对应的value的offset。若该分片已被预读，则从缓存中读取，否则从文件中读。内存中sortlog结构如下图所示：

![image](https://github.com/Qiwc/polardb-race/blob/master/images/4.jpg)

#### 4.3 range

采用经典的生产者消费者模型。程序对多线程range，设置了一个value的缓存区，缓存区可以容纳16个value分片，其中前8个缓存是可替换的，后8个缓存则是不可替换的，只能写入一次，用来减小读磁盘次数。

range开始时，会启动若干读磁盘线程（最终为2个线程），每个读磁盘线程会不断获取当前需要读的分片，然后判断缓存是否可用（是否被64个线程都range完），如果可用则读磁盘并写入，否则等待。

range线程则会依次处理每个分片，先判断当前分片有没有被读入到缓存中，如果没有则等待。在range时，由于sortlog中的key已经是有序的，只需依次取出key和offset，并从缓存中读出相应的value即可。value cache的内存模型如下图所示，其中active部分为ring cache。

![image](https://github.com/Qiwc/polardb-race/blob/master/images/5.jpg)

------

### 5 大赛总结

参加这次比赛，更让我懂得了学习要从原理出发。之前对NIO一知半解，不懂为什么这个快那个慢，于是学习了一波Linux IO机制，就ok啦。这次比赛能够入围最后的决赛离不开我们三人的努力，感谢两位队友，两位搞AI的和我这个半路出家的人一起搞起了程序设计比赛，实属不易。只要想做的就尽力去做，做了后悔总比不做后悔强！

