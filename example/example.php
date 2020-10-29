<?php


use yiiComponent\streamMq\StreamMQ;

$mq = new StreamMQ(['redis' => new Redis(), 'queueName' => 'testTest']);
$mq->setConsumerTypeCache(json_encode([
    'typeA' => 'consumerA',
    'typeB' => 'consumerB',
    'typeC' => 'consumerC',
    'typeD' => 'consumerD',
    'typeE' => 'consumerE',
    'typeF' => 'consumerF',
    'typeG' => 'consumerG',
    'typeH' => 'consumerH',
]));

/**
 * 入队
 * 推送1000条消息
 */
$success = [];
$type    = 'A';
for ($i = 1; $i <= 1000; $i++) {
    $message = [
        'type'   => 'type' . $type,
        'value'  => 'value' . $i,
        'status' => $i,
    ];
    $result = $mq->push($message);

    // 记录成功数量
    if ($result) {
        $success[$message['type']] += 1;
    }

    $type < 'H' ? $type = chr(ord($type)+1) : $type = 'A';
}

echo '入队测试结果：';
var_dump($success);


/**
 * 把消息读取到分组中，并按照类型分到不同的消费者中
 */
$groupResult = $mq->msgGroup();
echo '消息分组测试结果：';
var_dump($groupResult);

/**
 * 获取不同的消费者待处理消息
 */
$success = [];
$type    = 'A';
for ($i = ord($type); $i <= ord('H'); $i++) {
    $consumer = 'consumer' . chr($i);
    $start = '-';
    $end = '+';

    do {
        $data = $mq->readConsumerPendingMsg('mainGroup', $consumer, 20, $start, $end);
        $success[$consumer] += count($data);
        if ($start != '-') {
            $success[$consumer] -= 1;
        }

        if (!$data) {
            break;
        }

        $endArr = end($data);
        $start = $endArr[0];
    } while (count($data) > 1);
}

echo '获取不同的消费者待处理消息：';
var_dump($success);

/**
 * 消息处理（通过回调函数的方法去处理读取到的消息）
 * 执行xack函数
 * 返回处理后的结果
 */
$success = [];
$type = 'A';
for ($i = ord($type); $i <= ord('H'); $i++) {
    $consumer = 'consumer' . chr($i);
    $result = $mq->ackConsumerMsg($consumer, ['app\components\Consumer', 'calle']);
    $success[$consumer] = $result;
}

echo '测试消息处理（ack）的测试结果：';
var_dump($success);