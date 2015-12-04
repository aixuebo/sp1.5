一、项目名称
spark-branch-1.5-bak
二、git
git init
git add README.md
git commit -m "first commit"
git remote add origin https://github.com/aixuebo/sp1.5.git
git push -u origin master

三、storn存储逻辑
1、存储的基本单位是BlockId
2共存在8种BlockId
    case RDD(rddId, splitIndex) =>
      RDDBlockId(rddId.toInt, splitIndex.toInt)
    case SHUFFLE(shuffleId, mapId, reduceId) =>
      ShuffleBlockId(shuffleId.toInt, mapId.toInt, reduceId.toInt)
    case SHUFFLE_DATA(shuffleId, mapId, reduceId) =>
      ShuffleDataBlockId(shuffleId.toInt, mapId.toInt, reduceId.toInt)
    case SHUFFLE_INDEX(shuffleId, mapId, reduceId) =>
      ShuffleIndexBlockId(shuffleId.toInt, mapId.toInt, reduceId.toInt)
    case BROADCAST(broadcastId, field) =>
      BroadcastBlockId(broadcastId.toLong, field.stripPrefix("_"))
    case TASKRESULT(taskId) =>
      TaskResultBlockId(taskId.toLong)
    case STREAM(streamId, uniqueId) =>
      StreamBlockId(streamId.toInt, uniqueId.toLong)
    case TEST(value) =>
      TestBlockId(value)

3.BlockInfo(val level: StorageLevel, val tellMaster: Boolean)
  描述一个数据块的详细信息
  表示一个BlockId的状态是等待还是失败,以及该数据块完成的时候有多少个字节,以及该数据块存储在什么地方,是否通知master信息


4、BlockManagerId 一个执行者和所在的host-port组成一个数据块管理器,该管理器管理着自己执行环境的内存信息

5、BlockManager 表示BlockManagerId对应的具体类
   运行在每一个node上,每一个driver and executors都有一个该对象,管理属于该driver and executors的数据块信息,包括存储数据块和获取数据块,存储方式内存、磁盘、外部存储都可以


6.BlockUpdatedInfo 存储一个数据块在一个数据块管理着(block manager)里面的状态信息,用于org.apache.spark.scheduler.SparkListener里面监听SparkListenerBlockUpdated事件,用于更新数据块

7.BlockStatusListener 用于监听数据块更改事件
  管理每一个执行者的blockmanager 与之上面存储的BlockId以及block对应的状态

8.StorageLevel 定义一个存储方式,是存储在磁盘上、内存上、还是外部存储上

9.DiskBlockManager 磁盘数据块管理,该对象仅仅是管理数据块存储的File之间的关系,并不会真的产生写入文件操作
 a.创建path/blockmgr目录数组,可以多个磁盘下创建该目录
 b.传递一个数据块ID,获取该数据块ID应该存放在哪个目录下,放回要存放的File对象

10.DiskStore 类,与DiskBlockManager关联使用,通过DiskBlockManager进行获取数据块的文件所在位置,DiskStore类具体参与读取文件内容以及写入文件信息

11.BlockStore 存储数据块到文件系统或者内存的抽象类,专门用于定义存储接口,实现类有内存存储和磁盘存储
 实现类是DiskStore

12.


