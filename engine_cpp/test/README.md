## 简介 
本代码为初赛评测程序样例，旨在方便参赛者在提交代码前进行性能自测。

** 注：此样例不等同于实际线上评测逻辑，相对实际评测代码仅保留性能评测部分并做了简化 **

## 如何使用
1. 在项目目录下clone自己fork并实现的engine仓库
如：

```
git clone git@code.aliyun.com:XXX/engine.git

```

2. 执行 make

3. 运行

```

sudo TEST_TMPDIR=./test_benchmark ./benchmark

```

将TEST\_TMPDIR 设置为实际db\_path即可

更多参数具体用法见benchmark.cc

*本样例代码参考RocksDB db_bench_tool实现*
