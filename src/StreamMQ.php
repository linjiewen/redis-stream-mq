<?php

namespace yiiComponent\streamMq;

use yii\base\InvalidConfigException;
use Redis;

/**
 * Class StreamMQ
 *
 * @package app\components
 */
class StreamMQ extends \app\base\BaseComponent
{
    /**
     * @var string $queueName 队列名称
     */
    public $queueName;

    /**
     * @var object $redis Redis
     */
    public $redis;

    /**
     * @var int $readGroupNum 每次组读取数量
     */
    public $readGroupNum = 50;

    /**
     * @var string $group 默认分组
     */
    protected $group = 'mainGroup';

    /**
     * @var string $consumer 默认消费者
     */
    protected $consumer = 'mainConsumer';

    /**
     * @var string $typeKey 分组类型光健字
     */
    protected $typeKey = 'type';

    /**
     * @var string $consumerTypeCacheKey 消费者KEY和数据类型名称关系缓存KEY（注意要拼接上队列名称，一起使用）；
     */
    protected $consumerTypeCacheKey = 'mainGroupConsumers';


    /**
     * StreamMQ constructor.
     *
     * @param array $config
     * @throws InvalidConfigException
     */
    public function __construct(array $config = [])
    {
        parent::__construct($config);

        if (!$this->redis) {
            $this->redis = new Redis;
            $connect     = '127.0.0.1';
            $port        = 6379;
            $this->redis->connect($connect, $port);
        }

        if (!$this->queueName) {
            throw new InvalidConfigException('QueueName parameter cannot be null.');
        }

        // 创建原始分组
        $group = $this->redis->xInfo('GROUPS', $this->queueName);
        $group = $group ? array_column($group, 'name') : [];
        if (!in_array($this->group, $group)) {
            if (!$this->redis->xInfo('STREAM', $this->queueName)) {
                // 创建队列
                if (!$this->push(['value' => 'this is test'])) {
                    throw new InvalidConfigException('create queue fail.');
                }
            }

            // 创建消费分组
            if (!$this->createGroup($this->group)) {
                throw new InvalidConfigException('create group fail.');
            }
        }
    }

    /**
     * 消息分组
     *
     * @return array
     */
    public function msgGroup()
    {
        $allMsgNum   = 0;
        $claimMsgNum = 0;
        $consumers   = json_decode($this->redis->get($this->queueName . $this->consumerTypeCacheKey), true); // ['typeName' => 'consumer1']

        do {
            // 默认组读取消息
            $list = $this->readGroupMsg($this->group, $this->consumer, [], $this->readGroupNum);
            $data = $list[$this->queueName] ? $list[$this->queueName] : [];
            if ($data && $consumers) {
                // 是否需要分到不同的消费者
                $typeKeys = array_keys($consumers);
                $consumerDataIds = [];
                foreach ($data as $key => $value) {
                    if (in_array($value[$this->typeKey], $typeKeys)) {
                        $consumerDataIds[$value[$this->typeKey]][] = $key;
                    }
                }

                // 分组
                foreach ($consumerDataIds as $k => $v) {
                    $result = $this->xClaim($this->group, $consumers[$k], 0, $v, []);
                    $claimMsgNum += count($result);
                }
            }

            $allMsgNum += count($data);
        } while ($data);

        return [
            'allMsgNum'   => $allMsgNum,
            'claimMsgNum' => $claimMsgNum,
        ];
    }

    /**
     * 处理消费者消息
     *
     * @param  string $consumerName 消费者名称
     * @param  array  $callback     回调函数
     * @return int|void
     */
    public function ackConsumerMsg($consumerName, $callback)
    {
        $start   = 0;
        $success = 0;

        do {
            $msgData = $this->readGroupMsg($this->group, $consumerName, [$this->queueName => $start], $this->readGroupNum);
            $msgData = $msgData ? $msgData[$this->queueName] : [];

            if (!$callback || count($callback) < 2 || !$msgData) {
                return $success;
            }

            $result = call_user_func_array([new $callback[0], $callback[1]], [[$msgData]]);
            // 删除消费者待处理消息 TODO:应该放到业务中的事务中处理；
            if ($result) {
                $ids = [];
                foreach ($msgData as $k => $value) {
                    $ids[] = $k;
                }

                if ($ids) {
                    $result = $this->ack($this->group, $ids);
                    $success = !$result ?: $success + $result;
                    $start = end($ids);
                }
            }
        } while ($msgData);

        return $success;
    }

    /**
     * 转移消息的归属权
     *
     * @param  string $groupName    分组名称
     * @param  string $consumerName 消费者名称
     * @param  string $minIdletime  最小IDLE时间
     * @param  array  $ids          消息ID
     * @param  array  $options      ['IDLE' => $value, 'TIME' => $value, 'RETRYCOUNT' => $value, 'FORCE', 'JUSTID']
     * @return array
     */
    public function xClaim($groupName, $consumerName, $minIdletime, $ids, $options = [])
    {
        return $this->redis->xClaim($this->queueName, $groupName, $consumerName, $minIdletime, $ids, $options);
    }

    /**
     * 推送消息
     *
     * @param  array  $message 消息
     * @param  string $id      ID
     * @return string
     */
    public function push($message, $id = '*')
    {
        return $this->redis->xAdd($this->queueName, $id, $message);
    }

    /**
     * 删除
     *
     * @param  string|array $id ID
     * @return void
     */
    public function del($id)
    {
        if (!is_array($id)) {
            $id = [$id];
        }

        return $this->redis->xDel($this->queueName, $id);
    }

    /**
     * 倒序读取消息
     *
     * @param  string [$end = '+']    结束位置,'+'表示结束位置
     * @param  string [$start = '-']  开始位置,'-'表示从0开始
     * @param  int    [$count = null] 读取总数
     * @return array
     */
    public function readMsgDesc($end = '+', $start = '-', $count = null)
    {
        return $this->redis->xRevRange($this->queueName, $end, $start, $count);
    }

    /**
     * 正序读取消息
     *
     * @param  string [$start = '-']  开始位置,'-'表示从0开始
     * @param  string [$end = '+']    结束位置,'+'表示结束位置
     * @param  int    [$count = null] 读取总数
     * @return array
     */
    public function readMsgAsc($start = '-', $end = '+', $count = null)
    {
        return $this->redis->xRange($this->queueName, $start, $end, $count);
    }

    /**
     * 分组
     *
     * @param  string $operation   操作：'HELP', 'SETID', 'DELGROUP', 'CREATE', 'DELCONSUMER'
     * @param  string $groupName   分组名称
     * @param  string [$id = null] ID
     * @return mixed
     */
    public function group($operation, $groupName, $id = null)
    {
        return $this->redis->xGroup($operation, $this->queueName, $groupName, $id);
    }

    /**
     * 创建分组
     *
     * @param  string $groupName   分组名称
     * @return mixed
     */
    public function createGroup($groupName, $id = '$')
    {
        return $this->redis->xGroup('CREATE', $this->queueName, $groupName, $id);
    }

    /**
     * 删除分组
     *
     * @param  string $groupName   分组名称
     * @return mixed
     */
    public function delGroup($groupName)
    {
        return $this->redis->xGroup('DELGROUP', $this->queueName, $groupName, '$');
    }

    /**
     * 删除消费者
     *
     * @param  string $consumerName 消费者名称
     * @return mixed
     */
    public function delConsumer($consumerName)
    {
        return $this->redis->xGroup('DELCONSUMER', $this->queueName, $consumerName, '$');
    }

    /**
     * 设置分组最后投递ID
     *
     * @param  string $groupName 分组名称
     * @param  string $id        ID
     * @return mixed
     */
    public function setGroupLastDeliveredId($groupName, $id)
    {
        return $this->redis->xGroup('SETID', $this->queueName, $groupName, $id);
    }

    /**
     * 读取分组消息
     *
     * @param  string $groupName      分组名称
     * @param  string $consumerName   消费者名称
     * @param  array  $queues         队列
     * @param  int    [$count = null] 读取总数
     * @param  int    [$block = null] 阻塞时间
     * @return array
     */
    public function readGroupMsg($groupName, $consumerName, $queues = [], $count = 10, $block = null)
    {
        if (!$queues) {
            $queues = [$this->queueName => '>'];
        }

        return $this->redis->xReadGroup($groupName, $consumerName, $queues, $count, $block);
    }

    /**
     * 标志已经处理队列消息
     *
     * @param  string $groupName 分组名称
     * @param  array  $ids       ID数组
     * @return int
     */
    public function ack($groupName, $ids)
    {
        return $this->redis->xAck($this->queueName, $groupName, $ids);
    }

    /**
     * 读取消费者待处理消息
     *
     * @param  string $groupName      分组名称
     * @param  string $consumerName   消费者名称
     * @param  int    [$count = null] 读取总数
     * @param  string [$start = '-']  开始位置,'-'表示从0开始
     * @param  string [$end = '+']    结束位置,'+'表示结束位置
     * @return array
     */
    public function readConsumerPendingMsg($groupName, $consumerName = null, $count = null, $start = '-', $end = '+')
    {
        return $this->redis->xPending($this->queueName, $groupName, $start, $end, $count, $consumerName);
    }

    /**
     * 设置消费者和类型名称的关系
     *
     * @param  string $value 值
     * @return bool
     */
    public function setConsumerTypeCache($value)
    {
        return $this->redis->set($this->queueName . $this->consumerTypeCacheKey, $value);
    }
}
