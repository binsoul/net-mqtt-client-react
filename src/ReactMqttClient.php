<?php

namespace BinSoul\Net\Mqtt\Client\React;

use BinSoul\Net\Mqtt\Packet;
use BinSoul\Net\Mqtt\Packet\ConnectRequestPacket;
use BinSoul\Net\Mqtt\Packet\ConnectResponsePacket;
use BinSoul\Net\Mqtt\Packet\DisconnectRequestPacket;
use BinSoul\Net\Mqtt\Packet\PingRequestPacket;
use BinSoul\Net\Mqtt\Packet\PublishAckPacket;
use BinSoul\Net\Mqtt\Packet\PublishCompletePacket;
use BinSoul\Net\Mqtt\Packet\PublishReceivedPacket;
use BinSoul\Net\Mqtt\Packet\PublishReleasePacket;
use BinSoul\Net\Mqtt\Packet\PublishRequestPacket;
use BinSoul\Net\Mqtt\Packet\SubscribeRequestPacket;
use BinSoul\Net\Mqtt\Packet\SubscribeResponsePacket;
use BinSoul\Net\Mqtt\Packet\UnsubscribeRequestPacket;
use BinSoul\Net\Mqtt\Packet\UnsubscribeResponsePacket;
use BinSoul\Net\Mqtt\StreamParser;
use Evenement\EventEmitter;
use React\EventLoop\Timer\TimerInterface;
use React\Promise\Deferred;
use React\EventLoop\LoopInterface;
use React\Promise\ExtendedPromiseInterface;
use React\Promise\FulfilledPromise;
use React\SocketClient\ConnectorInterface;
use React\Stream\Stream;

/**
 * Connects to a MQTT broker and subscribes to topics or publishes messages.
 *
 * The following events are emitted:
 *  - connect - The connection to the broker is established.
 *  - disconnect  - The connection to the broker is closed.
 *  - message - An incoming message is received.
 *  - warning - An event of severity "warning" occurred.
 *  - error - An event of severity "error" occurred.
 */
class ReactMqttClient extends EventEmitter
{
    /** @var Stream */
    private $stream;
    /** @var LoopInterface */
    private $loop;
    /** @var StreamParser */
    private $parser;
    /** @var bool */
    private $isConnected = false;
    /** @var Deferred */
    private $connectDeferred;
    /** @var TimerInterface */
    private $connectTimer;

    /** @var TimerInterface[] */
    private $timer = [];
    /** @var Deferred[][] */
    private $deferred = [
        'subscribe' => [],
        'unsubscribe' => [],
        'publish' => [],
    ];
    /** @var string[] */
    private $subscribe = [];
    /** @var string[] */
    private $unsubscribe = [];
    /** @var PublishRequestPacket[] */
    private $publishQos1 = [];
    /** @var PublishRequestPacket[] */
    private $publishQos2 = [];
    /** @var PublishRequestPacket[] */
    private $incomingQos2 = [];

    /**
     * Constructs an instance of this class.
     *
     * @param ConnectorInterface $connector
     * @param LoopInterface      $loop
     * @param StreamParser       $parser
     */
    public function __construct(ConnectorInterface $connector, LoopInterface $loop, StreamParser $parser = null)
    {
        $this->connector = $connector;
        $this->loop = $loop;

        $this->parser = $parser;
        if ($this->parser === null) {
            $this->parser = new StreamParser();
        }

        $this->parser->onError(function (\Exception $e) {
            $this->emitWarning($e);
        });
    }

    /**
     * Indicates if the client is connected.
     *
     * @return bool
     */
    public function isConnected()
    {
        return $this->isConnected;
    }

    /**
     * Connects to a broker.
     *
     * @param string  $host
     * @param int     $port
     * @param mixed[] $options
     * @param int     $timeout
     * @param int     $keepAlive
     *
     * @return ExtendedPromiseInterface
     */
    public function connect($host, $port = 1883, array $options = [], $timeout = 5, $keepAlive = 60)
    {
        if ($this->isConnected) {
            $this->disconnect();
        }

        $settings = $this->sanitizeOptions($options);

        $this->connectDeferred = new Deferred();
        $timer = $this->loop->addTimer(
            $timeout,
            function () use ($timeout) {
                $exception = new \RuntimeException(sprintf('Connection timed out after %d seconds.', $timeout));
                $this->emitError($exception);
                $this->connectDeferred->reject($exception);
                $this->emit('disconnect', [$this]);
                $this->loop->stop();
            }
        );

        $this->connector->create($host, $port)->then(
            function (Stream $stream) use ($timer, $settings, $timeout, $keepAlive) {
                $this->loop->cancelTimer($timer);

                $this->connectStream($stream, $settings, $timeout, $keepAlive);
            },
            function (\Exception $e) {
                $this->connectDeferred->reject($e);
            }
        );

        return $this->connectDeferred->promise();
    }

    /**
     * Disconnects the client.
     */
    public function disconnect()
    {
        if (!$this->isConnected) {
            return;
        }

        $packet = new DisconnectRequestPacket();

        $this->stream->write($packet);
        $this->stream->close();
    }

    /**
     * @param string $topic
     * @param int    $qosLevel
     *
     * @return ExtendedPromiseInterface
     */
    public function subscribe($topic, $qosLevel = 0)
    {
        $packet = new SubscribeRequestPacket();
        $packet->setTopic($topic);
        $packet->setQosLevel($qosLevel);

        $this->stream->write($packet);

        $id = $packet->getIdentifier();
        $this->deferred['subscribe'][$id] = new Deferred();
        $this->subscribe[$id] = [$topic];

        return $this->deferred['subscribe'][$id]->promise();
    }

    /**
     * @param string $topic
     *
     * @return ExtendedPromiseInterface
     */
    public function unsubscribe($topic)
    {
        $packet = new UnsubscribeRequestPacket();
        $packet->setTopic($topic);

        $this->stream->write($packet);

        $id = $packet->getIdentifier();
        $this->deferred['unsubscribe'][$id] = new Deferred();
        $this->unsubscribe[$id] = [$topic];

        return $this->deferred['unsubscribe'][$id]->promise();
    }

    /**
     * @param string $topic
     * @param string $message
     * @param int    $qosLevel
     * @param bool   $retain
     *
     * @return ExtendedPromiseInterface
     */
    public function publish($topic, $message, $qosLevel = 0, $retain = false)
    {
        $packet = new PublishRequestPacket();
        $packet->setTopic($topic);
        $packet->setPayload($message);
        $packet->setQosLevel($qosLevel);
        $packet->setRetained($retain);
        $packet->setDuplicate(false);

        $this->stream->write($packet);

        if ($qosLevel == 0) {
            return new FulfilledPromise($message);
        }

        $id = $packet->getIdentifier();
        $this->deferred['publish'][$id] = new Deferred();
        if ($qosLevel == 1) {
            $this->publishQos1[$id] = $packet;
        } else {
            $this->publishQos2[$id] = $packet;
        }

        return $this->deferred['publish'][$id]->promise();
    }

    /**
     * Calls the given generator periodically and publishes the return value.
     *
     * @param int      $interval
     * @param string   $topic
     * @param callable $generator
     * @param int      $qosLevel
     * @param bool     $retain
     *
     * @return ExtendedPromiseInterface
     */
    public function publishPeriodically($interval, $topic, callable $generator, $qosLevel = 0, $retain = false)
    {
        $deferred = new Deferred();

        $this->timer[] = $this->loop->addPeriodicTimer(
            $interval,
            function () use ($topic, $generator, $qosLevel, $retain, $deferred) {
                $this->publish($topic, $generator($topic), $qosLevel, $retain)->then(
                    function ($value) use ($deferred) {
                        $deferred->notify($value);
                    },
                    function (\Exception $e) use ($deferred) {
                        $deferred->reject($e);
                    }
                );
            }
        );

        return $deferred->promise();
    }

    /**
     * @param Stream  $stream
     * @param mixed[] $settings
     * @param int     $timeout
     * @param int     $keepAlive
     */
    private function connectStream(Stream $stream, array $settings, $timeout, $keepAlive)
    {
        $this->stream = $stream;
        $this->stream->on('data', function ($data) {
            $this->handleData($data);
        });

        $this->stream->on('error', function (\Exception $e) {
            $this->emitError($e);
        });

        $this->stream->on('close', function () {
            foreach ($this->timer as $timer) {
                $this->loop->cancelTimer($timer);
            }

            $this->isConnected = false;
            $this->emit('disconnect', [$this]);
        });

        $this->timer[] = $this->loop->addPeriodicTimer(
            floor($keepAlive * 0.75),
            function () {
                $this->stream->write(new PingRequestPacket());
            }
        );

        $this->connectTimer = $this->loop->addTimer(
            $timeout,
            function () use ($timeout) {
                $exception = new \RuntimeException(sprintf('Connection timed out after %d seconds.', $timeout));
                $this->emitError($exception);
                $this->connectDeferred->reject($exception);

                $this->stream->close();
            }
        );

        $packet = new ConnectRequestPacket();
        $packet->setProtocolLevel($settings['protocol']);
        $packet->setKeepAlive($keepAlive);
        $packet->setClientID($settings['clientID']);
        $packet->setCleanSession($settings['clean']);
        $packet->setUsername($settings['username']);
        $packet->setPassword($settings['password']);
        $will = $settings['will'];
        if ($will['topic'] != '' && $will['message'] != '') {
            $packet->setWill($will['topic'], $will['message'], $will['qos'], $will['retain']);
        }

        $this->stream->write($packet);
    }

    /**
     * Handles incoming data.
     *
     * @param string $data
     */
    private function handleData($data)
    {
        $packets = $this->parser->push($data);
        foreach ($packets as $packet) {
            switch ($packet->getPacketType()) {
                case Packet::TYPE_CONNACK:
                    $this->executeConnectResponse($packet);
                    break;
                case Packet::TYPE_SUBACK:
                    $this->executeSubscribeResponse($packet);
                    break;
                case Packet::TYPE_UNSUBACK:
                    $this->executeUnsubscribeResponse($packet);
                    break;
                case Packet::TYPE_PUBLISH:
                    $this->executePublishRequest($packet);
                    break;
                case Packet::TYPE_PUBACK:
                    $this->executePublishAck($packet);
                    break;
                case Packet::TYPE_PUBREC:
                    $this->executePublishReceived($packet);
                    break;
                case Packet::TYPE_PUBREL:
                    $this->executePublishRelease($packet);
                    break;
                case Packet::TYPE_PUBCOMP:
                    $this->executePublishComplete($packet);
                    break;
                case Packet::TYPE_PINGRESP:
                    break;
                default:
                    $this->emitWarning(
                        new \RuntimeException(sprintf('Cannot handle packet of type %d.', $packet->getPacketType()))
                    );
            }
        }
    }

    /**
     * Emits messages.
     *
     * @param PublishRequestPacket $packet
     */
    private function emitMessage(PublishRequestPacket $packet)
    {
        $this->emit(
            'message',
            [
                $packet->getTopic(),
                $packet->getPayload(),
                $packet->isDuplicate(),
                $packet->isRetained(),
            ]
        );
    }

    /**
     * Emits warnings.
     *
     * @param \Exception $e
     */
    private function emitWarning(\Exception $e)
    {
        $this->emit('warning', [$e]);
    }

    /**
     * Emits errors.
     *
     * @param \Exception $e
     */
    private function emitError(\Exception $e)
    {
        $this->emit('error', [$e]);
    }

    /**
     * Sanitizes the user provided options.
     *
     * @param mixed[] $options
     *
     * @return mixed[]
     */
    private function sanitizeOptions(array $options)
    {
        $defaults = [
            'protocol' => 4,
            'clientID' => '',
            'clean' => true,
            'username' => '',
            'password' => '',
            'will' => [
                'topic' => '',
                'message' => '',
                'qos' => 0,
                'retain' => false,
            ],
        ];

        return array_merge($defaults, $options);
    }

    /**
     * @param ConnectResponsePacket $packet
     */
    private function executeConnectResponse(ConnectResponsePacket $packet)
    {
        $this->loop->cancelTimer($this->connectTimer);

        if ($packet->isSuccess()) {
            $this->isConnected = true;
            $this->emit('connect', [$this]);
            $this->connectDeferred->resolve($this);
        } else {
            $exception = new \RuntimeException($packet->getErrorName());
            $this->emitError($exception);
            $this->connectDeferred->reject($exception);
            $this->stream->close();
        }
    }

    /**
     * @param SubscribeResponsePacket $packet
     */
    private function executeSubscribeResponse(SubscribeResponsePacket $packet)
    {
        $id = $packet->getIdentifier();
        if (isset($this->subscribe[$id])) {
            $returnCodes = $packet->getReturnCodes();
            foreach ($returnCodes as $index => $returnCode) {
                $topic = $this->subscribe[$id][$index];
                if ($packet->isError($returnCode)) {
                    $this->deferred['subscribe'][$id]->reject(
                        new \RuntimeException(sprintf('Cannot subscribe to topic "%s".', $topic))
                    );
                } else {
                    $this->deferred['subscribe'][$id]->resolve($topic);
                }

                unset($this->deferred['subscribe'][$id]);
            }
        } else {
            $this->emitWarning(
                new \LogicException(
                    sprintf(
                        'Subscribe packet identifier %d not found.',
                        $id
                    )
                )
            );
        }
    }

    /**
     * @param UnsubscribeResponsePacket $packet
     */
    private function executeUnsubscribeResponse(UnsubscribeResponsePacket $packet)
    {
        $id = $packet->getIdentifier();
        if (isset($this->unsubscribe[$id])) {
            $topic = $this->unsubscribe[$id];
            $this->deferred['unsubscribe'][$id]->resolve($topic);

            unset($this->unsubscribe[$id]);
            unset($this->deferred['unsubscribe'][$id]);
        } else {
            $this->emitWarning(
                new \LogicException(
                    sprintf(
                        'Unsubscribe packet identifier %d not found.',
                        $id
                    )
                )
            );
        }
    }

    /**
     * @param PublishRequestPacket $packet
     */
    private function executePublishRequest(PublishRequestPacket $packet)
    {
        $response = null;
        $emit = true;
        if ($packet->getQosLevel() == 1) {
            $response = new PublishAckPacket();
        } elseif ($packet->getQosLevel() == 2) {
            $response = new PublishReceivedPacket();
            $this->incomingQos2[$packet->getIdentifier()] = $packet;
            $emit = false;
        }

        if ($response !== null) {
            $response->setIdentifier($packet->getIdentifier());
            $this->stream->write($response);
        }

        if ($emit) {
            $this->emitMessage($packet);
        }
    }

    /**
     * @param PublishAckPacket $packet
     */
    private function executePublishAck(PublishAckPacket $packet)
    {
        $id = $packet->getIdentifier();
        if (isset($this->publishQos1[$id])) {
            $this->deferred['publish'][$id]->resolve($this->publishQos1[$id]->getPayload());
            unset($this->publishQos1[$id]);
            unset($this->deferred['publish'][$id]);
        } else {
            $this->emitWarning(
                new \LogicException(
                    sprintf(
                        'PUBACK: Publish packet identifier %d not found.',
                        $id
                    )
                )
            );
        }
    }

    /**
     * @param PublishReceivedPacket $packet
     */
    private function executePublishReceived(PublishReceivedPacket $packet)
    {
        $id = $packet->getIdentifier();
        if (isset($this->publishQos2[$id])) {
            $response = new PublishReleasePacket();
            $response->setIdentifier($id);
            $this->stream->write($response);
        } else {
            $this->emitWarning(
                new \LogicException(
                    sprintf(
                        'PUBREC: Publish packet identifier %d not found.',
                        $id
                    )
                )
            );
        }
    }

    /**
     * @param PublishReleasePacket $packet
     */
    private function executePublishRelease(PublishReleasePacket $packet)
    {
        $id = $packet->getIdentifier();

        $response = new PublishCompletePacket();
        $response->setIdentifier($id);
        $this->stream->write($response);

        if (isset($this->incomingQos2[$id])) {
            $this->emitMessage($this->incomingQos2[$id]);
            unset($this->incomingQos2[$id]);
        } else {
            $this->emitWarning(
                new \LogicException(
                    sprintf(
                        'PUBREL: Incoming packet identifier %d not found.',
                        $id
                    )
                )
            );
        }
    }

    /**
     * @param PublishCompletePacket $packet
     */
    private function executePublishComplete(PublishCompletePacket $packet)
    {
        $id = $packet->getIdentifier();
        if (isset($this->publishQos2[$id])) {
            $this->deferred['publish'][$id]->resolve($this->publishQos2[$id]->getPayload());
            unset($this->publishQos2[$id]);
            unset($this->deferred['publish'][$id]);
        } else {
            $this->emitWarning(
                new \LogicException(
                    sprintf(
                        'PUBCOMP: Publish packet identifier %d not found.',
                        $id
                    )
                )
            );
        }
    }
}
