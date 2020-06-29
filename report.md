# <center>云计算项目实践</center>

### <center>17341043</center>

### <center>郭工瑶</center>

### <center>数据科学与计算机学院</center>

#### 一.实验内容
选项1、基于Hadoop平台设计编写一个MapReduce程序，具体功能自行确定。
选项2、基于Kubernetes平台的功能拓展
所选题目：distributed sort&&simple kmeans
#### 二.实验环境
中大超算平台
![ ](image/1.png)
#### 三.实验流程
- 根据之前的实验二所做的内容wordcount以及invertedindex复习mapreduce实现过程map和reduce操作，以及编译流程和实现过程。

- 用mapreduce实现sort
  输入数据每一行一个随机数，输出按从小到大排序对用[index  value]键值对,代码见sortdata/SortedData.java,详细[代码](/sortdata/SortedData.java)
  ![ ](image/2.png)
  ![ ](image/3.png)
  ![ ](image/4.png)

- 用mapreduce实现kmeans
参考 https://blog.csdn.net/garfielder007/article/details/51612730
根据他的思路我把代码分块,按他的思路更进一步理解。
Kmeans通俗理解：
选择K个点作为初始质心
repeat
    将每个点指派到最近的质心，形成K个簇
    重新计算每个簇的质心
until 簇不发生变化或达到最大迭代次数

基本思想：
1. 用一个全局变量存放上一次迭代的质心
2. map()中，计算每个质心与样本之间的距离，得到与样本距离最短的质心，以这个质心作为key，样本作为value输出。
3. Reduce()阶段：输入的key是质心，value是其他的样本，这时重新计算聚类中心，将聚类中心put到一个全部变量t中
4. 在main()中比较前一次的质心和本次的质心是否发生变化，如果变化，则继续迭代，否则退出。

注意点：
1. Hadoop是不存在自定义的全局变量的，所以上面定义一个全局变量存放质心的想法是实现不了的，所以一个替代的思路是将质心存放在文件中

2. 存放质心的文件在什么地方读取，如果在map中读取，那么可以肯定我们是不能用一个mapreduce实现一次迭代，所以我们选择在main函数里读取质心，然后将质心set到configuration中，configuration在map和reduce都是可读

3. 如何比较质心是否发生变化，是在main里比较么，读取本次质心和上一次质心的文件然后进行比较，这种方法是可以实现的，但是显得不够好，稍显繁琐这个时候我们用到了自定义的counter，counter是全局变量，在map和reduce中可读可写，在上面的思路中，我们看到reduce是有上次迭代的质心和刚刚计算出来的质心的，所以直接在reduce中进行比较就完全可以，如果没发生变化，counter加1。只要在main里比较获取counter的值就行了。

具体实现：
1. main函数读取质心文件
2. 将质心的字符串放到configuration中
3. 在mapper类重写setup方法，获取到configuration的质心内容，解析成二维数组的形式，代表质心

4. mapper类中的map方法读取样本文件，跟所有的质心比较，得出每个样本跟哪个质心最近，然后输出<质心，样本>

5. reducer类中重新计算质心，如果重新计算出来的质心跟进来时的质心一致，那么自定义的counter加1

6. main中获取counter的值，看是否等于质心，如果不相等，那么继续迭代，否则退出

代码结构：
[Center.java](kmeans/Center.java):设置初始样本质心以及迭代后的质心

[TokenizerMap](kmeans/TokenizerMapper.java):计算样本点中最近的质心，形成蔟（聚类）

[IntSumReduce.java](kmeans/IntSumReducer.java):根据聚类生成新的质心并且判断新的质心是否于原来相同，然后进行对应操作

[Run.java](kmeans/Run.java):设定输入输出，整合并且迭代求出最终质心。
```
javac Run.java
jar cvf Run.jar ./Run.class TokenizerMapper.class IntSumReducer.class Center.class
/usr/local/hadoop/bin/hadoop jar Run.jar Run
```
实验结果：
为了方便比较，我也使用他所提供的测试样例，判断结果。
  ![ ](image/5.png)
  初始质心：
  ![ ](image/6.png)
  样本点：
  ![ ](image/7.png)
  输出结果：三个文件（设定迭代五次，说明第二次迭代后结果不变了）
  ![ ](image/11.png)
  三个文件夹对应结果：最终结果一致
  ![ ](image/8.png)
  ![ ](image/9.png)
  ![ ](image/10.png)
以上所有代码详情可见打包文件或者 https://github.com/ggyhug/Mapreduce
#### 四.实验心得
这个实验主要了解了基于hadoop的mapreduce实例，进行了进一步的操作以及学习，过程中进一步熟练运用linux相关操作以及java语法的基本知识，这次实验代码的编写过程中就有许多关于java一些语法的基本知识，虽然之前接触过一些，但是实际运用起来还是出现了一些问题，通过javac编译时，一些基本的语法错误还是暴露了java基本知识的不牢固，同时也遇到了很多低级的错误，比如类声明，变量类型不对，导致不能读入，不能相互转换，还有虽然编译时通过，后来仔细看输出结果是因为reduce过程中传入值错误，map 100%， reduce 0%，总的来说这次实验还是收获很多，再之前的基础之上，可以说是进一步熟悉map-reduce这个过程，理解mapreduce分布式框架下如何并行处理，分布式运算，学会将问题拆分合并，云计算课程的结束，也是收获满满，可以说是对linux操作有了巨大提升，而且对hadoop、kubernetes这两个并行框架，也是现代主流技术有了一个比较实际的体验过程，能进行基本的操作，掌握了基本的操作方法以及原理过程，同时也简单学习了java、go等语言基本语法知识，对将来学习帮助很大。
