һ����Ŀ����
spark-branch-1.5-bak
����git
git init
git add README.md
git commit -m "first commit"
git remote add origin https://github.com/aixuebo/sp1.5.git
git push -u origin master

����storn�洢�߼�
1���洢�Ļ�����λ��BlockId
2������8��BlockId
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
  ����һ�����ݿ����ϸ��Ϣ
  ��ʾһ��BlockId��״̬�ǵȴ�����ʧ��,�Լ������ݿ���ɵ�ʱ���ж��ٸ��ֽ�,�Լ������ݿ�洢��ʲô�ط�,�Ƿ�֪ͨmaster��Ϣ


4��BlockManagerId һ��ִ���ߺ����ڵ�host-port���һ�����ݿ������,�ù������������Լ�ִ�л������ڴ���Ϣ

5��BlockManager ��ʾBlockManagerId��Ӧ�ľ�����
   ������ÿһ��node��,ÿһ��driver and executors����һ���ö���,�������ڸ�driver and executors�����ݿ���Ϣ,�����洢���ݿ�ͻ�ȡ���ݿ�,�洢��ʽ�ڴ桢���̡��ⲿ�洢������


6.BlockUpdatedInfo �洢һ�����ݿ���һ�����ݿ������(block manager)�����״̬��Ϣ,����org.apache.spark.scheduler.SparkListener�������SparkListenerBlockUpdated�¼�,���ڸ������ݿ�

7.BlockStatusListener ���ڼ������ݿ�����¼�
  ����ÿһ��ִ���ߵ�blockmanager ��֮����洢��BlockId�Լ�block��Ӧ��״̬

8.StorageLevel ����һ���洢��ʽ,�Ǵ洢�ڴ����ϡ��ڴ��ϡ������ⲿ�洢��

9.DiskBlockManager �������ݿ����,�ö�������ǹ������ݿ�洢��File֮��Ĺ�ϵ,��������Ĳ���д���ļ�����
 a.����path/blockmgrĿ¼����,���Զ�������´�����Ŀ¼
 b.����һ�����ݿ�ID,��ȡ�����ݿ�IDӦ�ô�����ĸ�Ŀ¼��,�Ż�Ҫ��ŵ�File����

10.DiskStore ��,��DiskBlockManager����ʹ��,ͨ��DiskBlockManager���л�ȡ���ݿ���ļ�����λ��,DiskStore���������ȡ�ļ������Լ�д���ļ���Ϣ

11.BlockStore �洢���ݿ鵽�ļ�ϵͳ�����ڴ�ĳ�����,ר�����ڶ���洢�ӿ�,ʵ�������ڴ�洢�ʹ��̴洢
 ʵ������DiskStore

12.


