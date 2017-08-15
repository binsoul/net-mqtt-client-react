<?php

namespace BinSoul\Net\Mqtt\Client\React;

use BinSoul\Net\Mqtt\Connection;
use BinSoul\Net\Mqtt\DefaultConnection;
use BinSoul\Net\Mqtt\DefaultIdentifierGenerator;
use BinSoul\Net\Mqtt\Flow;
use BinSoul\Net\Mqtt\Flow\IncomingPublishFlow;
use BinSoul\Net\Mqtt\Flow\OutgoingConnectFlow;
use BinSoul\Net\Mqtt\Flow\OutgoingDisconnectFlow;
use BinSoul\Net\Mqtt\Flow\OutgoingPingFlow;
use BinSoul\Net\Mqtt\Flow\OutgoingPublishFlow;
use BinSoul\Net\Mqtt\Flow\OutgoingSubscribeFlow;
use BinSoul\Net\Mqtt\Flow\OutgoingUnsubscribeFlow;
use BinSoul\Net\Mqtt\DefaultMessage;
use BinSoul\Net\Mqtt\IdentifierGenerator;
use BinSoul\Net\Mqtt\Message;
use BinSoul\Net\Mqtt\Packet;
use BinSoul\Net\Mqtt\Packet\PublishRequestPacket;
use BinSoul\Net\Mqtt\StreamParser;
use BinSoul\Net\Mqtt\Subscription;
use Evenement\EventEmitter;
use React\EventLoop\Timer\TimerInterface;
use React\Promise\Deferred;
use React\EventLoop\LoopInterface;
use React\Promise\ExtendedPromiseInterface;
use React\Promise\RejectedPromise;
use React\Socket\ConnectorInterface;
use React\Stream\DuplexStreamInterface;

/**
 * Connects to a MQTT broker and subscribes to topics or publishes messages.
 *
 * The following events are emitted:
 *  - open - The network connection to the server is established.
 *  - close - The network connection to the server is closed.
 *  - warning - An event of severity "warning" occurred.
 *  - error - An event of severity "error" occurred.
 *
 * If a flow finishes it's result is also emitted, e.g.:
 *  - connect - The client connected to the broker.
 *  - disconnect - The client disconnected from the broker.
 *  - subscribe - The client subscribed to a topic filter.
 *  - unsubscribe - The client unsubscribed from topic filter.
 *  - publish - A message was published.
 *  - message - A message was received.
 */
class ReactMqttClient extends EventEmitter
{
    /** @var ConnectorInterface */
    private $connector;
    /** @var LoopInterface */
    private $loop;
    /** @var DuplexStreamInterface */
    private $stream;
    /** @var StreamParser */
    private $parser;
    /** @var IdentifierGenerator */
    private $identifierGenerator;

    /** @var string */
    private $host;
    /** @var int */
    private $port;
    /** @var Connection */
    private $connection;
    /** @var bool */
    private $isConnected = false;
    /** @var bool */
    private $isConnecting = false;
    /** @var bool */
    private $isDisconnecting = false;

    /** @var TimerInterface[] */
    private $timer = [];

    /** @var ReactFlow[] */
    private $receivingFlows = [];
    /** @var ReactFlow[] */
    private $sendingFlows = [];
    /** @var ReactFlow */
    private $writtenFlow;

    /**
     * Constructs an instance of this class.
     *
     * @param ConnectorInterface  $connector
     * @param LoopInterface       $loop
     * @param IdentifierGenerator $identifierGenerator
     * @param StreamParser        $parser
     */
    public function __construct(
        ConnectorInterface $connector,
        LoopInterface $loop,
        IdentifierGenerator $identifierGenerator = null,
        StreamParser $parser = null
    ) {
        $this->connector = $connector;
        $this->loop = $loop;

        $this->parser = $parser;
        if ($this->parser === null) {
            $this->parser = new StreamParser();
        }

        $this->parser->onError(function (\Exception $e) {
            $this->emitWarning($e);
        });

        $this->identifierGenerator = $identifierGenerator;
        if ($this->identifierGenerator === null) {
            $this->identifierGenerator = new DefaultIdentifierGenerator();
        }
    }

    /**
     * Return the host.
     *
     * @return string
     */
    public function getHost()
    {
        return $this->host;
    }

    /**
     * Return the port.
     *
     * @return string
     */
    public function getPort()
    {
        return $this->port;
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
     * Returns the underlying stream or null if the client is not connected.
     *
     * @return DuplexStreamInterface|null
     */
    public function getStream()
    {
        return $this->stream;
    }

    /**
     * Connects to a broker.
     *
     * @param string     $host
     * @param int        $port
     * @param Connection $connection
     * @param int        $timeout
     *
     * @return ExtendedPromiseInterface
     */
    public function connect($host, $port = 1883, Connection $connection = null, $timeout = 5)
    {
        if ($this->isConnected || $this->isConnecting) {
            return new RejectedPromise(new \LogicException('The client is already connected.'));
        }

        $this->isConnecting = true;
        $this->isConnected = false;

        $this->host = $host;
        $this->port = $port;

        if ($connection === null) {
            $connection = new DefaultConnection();
        }

        if ($connection->getClientID() === '') {
            $connection = $connection->withClientID($this->identifierGenerator->generateClientID());
        }

        $deferred = new Deferred();

        $this->establishConnection($this->host, $this->port, $timeout)
            ->then(function (DuplexStreamInterface $stream) use ($connection, $deferred, $timeout) {
                $this->stream = $stream;

                $this->emit('open', [$connection, $this]);

                $this->registerClient($connection, $timeout)
                    ->then(function (Connection $connection) use ($deferred) {
                        $this->isConnecting = false;
                        $this->isConnected = true;
                        $this->connection = $connection;

                        $this->emit('connect', [$connection, $this]);
                        $deferred->resolve($this->connection);
                    })
                    ->otherwise(function (\Exception $e) use ($deferred, $connection) {
                        $this->isConnecting = false;

                        $this->emitError($e);
                        $deferred->reject($e);

                        if ($this->stream !== null) {
                            $this->stream->close();
                        }

                        $this->emit('close', [$connection, $this]);
                    });
            })
            ->otherwise(function (\Exception $e) use ($deferred) {
                $this->isConnecting = false;

                $this->emitError($e);
                $deferred->reject($e);
            });

        return $deferred->promise();
    }

    /**
     * Disconnects from a broker.
     *
     * @return ExtendedPromiseInterface
     */
    public function disconnect()
    {
        if (!$this->isConnected || $this->isDisconnecting) {
            return new RejectedPromise(new \LogicException('The client is not connected.'));
        }

        $this->isDisconnecting = true;

        $deferred = new Deferred();

        $this->startFlow(new OutgoingDisconnectFlow($this->connection), true)
            ->then(function (Connection $connection) use ($deferred) {
                $this->isDisconnecting = false;
                $this->isConnected = false;

                $this->emit('disconnect', [$connection, $this]);
                $deferred->resolve($connection);

                if ($this->stream !== null) {
                    $this->stream->close();
                }
            })
            ->otherwise(function () use ($deferred) {
                $this->isDisconnecting = false;
                $deferred->reject($this->connection);
            });

        return $deferred->promise();
    }

    /**
     * Subscribes to a topic filter.
     *
     * @param Subscription $subscription
     *
     * @return ExtendedPromiseInterface
     */
    public function subscribe(Subscription $subscription)
    {
        if (!$this->isConnected) {
            return new RejectedPromise(new \LogicException('The client is not connected.'));
        }

        return $this->startFlow(new OutgoingSubscribeFlow([$subscription], $this->identifierGenerator));
    }

    /**
     * Unsubscribes from a topic filter.
     *
     * @param Subscription $subscription
     *
     * @return ExtendedPromiseInterface
     */
    public function unsubscribe(Subscription $subscription)
    {
        if (!$this->isConnected) {
            return new RejectedPromise(new \LogicException('The client is not connected.'));
        }

        return $this->startFlow(new OutgoingUnsubscribeFlow([$subscription], $this->identifierGenerator));
    }

    /**
     * Publishes a message.
     *
     * @param Message $message
     *
     * @return ExtendedPromiseInterface
     */
    public function publish(Message $message)
    {
        if (!$this->isConnected) {
            return new RejectedPromise(new \LogicException('The client is not connected.'));
        }

        return $this->startFlow(new OutgoingPublishFlow($message, $this->identifierGenerator));
    }

    /**
     * Calls the given generator periodically and publishes the return value.
     *
     * @param int      $interval
     * @param Message  $message
     * @param callable $generator
     *
     * @return ExtendedPromiseInterface
     */
    public function publishPeriodically($interval, Message $message, callable $generator)
    {
        if (!$this->isConnected) {
            return new RejectedPromise(new \LogicException('The client is not connected.'));
        }

        $deferred = new Deferred();

        $this->timer[] = $this->loop->addPeriodicTimer(
            $interval,
            function () use ($message, $generator, $deferred) {
                $this->publish($message->withPayload($generator($message->getTopic())))->then(
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
     * Emits warnings.
     *
     * @param \Exception $e
     */
    private function emitWarning(\Exception $e)
    {
        $this->emit('warning', [$e, $this]);
    }

    /**
     * Emits errors.
     *
     * @param \Exception $e
     */
    private function emitError(\Exception $e)
    {
        $this->emit('error', [$e, $this]);
    }

    /**
     * Establishes a network connection to a server.
     *
     * @param string $host
     * @param int    $port
     * @param int    $timeout
     *
     * @return ExtendedPromiseInterface
     */
    private function establishConnection($host, $port, $timeout)
    {
        $deferred = new Deferred();

        $timer = $this->loop->addTimer(
            $timeout,
            function () use ($deferred, $timeout) {
                $exception = new \RuntimeException(sprintf('Connection timed out after %d seconds.', $timeout));
                $deferred->reject($exception);
            }
        );

        $this->connector->connect($host.':'.$port)
            ->always(function () use ($timer) {
                $this->loop->cancelTimer($timer);
            })
            ->then(function (DuplexStreamInterface $stream) use ($deferred) {
                $stream->on('data', function ($data) {
                    $this->handleReceive($data);
                });

                $stream->on('close', function () {
                    $this->handleClose();
                });

                $stream->on('error', function (\Exception $e) {
                    $this->handleError($e);
                });

                $deferred->resolve($stream);
            })
            ->otherwise(function (\Exception $e) use ($deferred) {
                $deferred->reject($e);
            });

        return $deferred->promise();
    }

    /**
     * Registers a new client with the broker.
     *
     * @param Connection $connection
     * @param int        $timeout
     *
     * @return ExtendedPromiseInterface
     */
    private function registerClient(Connection $connection, $timeout)
    {
        $deferred = new Deferred();

        $responseTimer = $this->loop->addTimer(
            $timeout,
            function () use ($deferred, $timeout) {
                $exception = new \RuntimeException(sprintf('No response after %d seconds.', $timeout));
                $deferred->reject($exception);
            }
        );

        $this->startFlow(new OutgoingConnectFlow($connection, $this->identifierGenerator), true)
            ->always(function () use ($responseTimer) {
                $this->loop->cancelTimer($responseTimer);
            })->then(function (Connection $connection) use ($deferred) {
                $this->timer[] = $this->loop->addPeriodicTimer(
                    floor($connection->getKeepAlive() * 0.75),
                    function () {
                        $this->startFlow(new OutgoingPingFlow());
                    }
                );

                $deferred->resolve($connection);
            })->otherwise(function (\Exception $e) use ($deferred) {
                $deferred->reject($e);
            });

        return $deferred->promise();
    }

    /**
     * Handles incoming data.
     *
     * @param string $data
     */
    private function handleReceive($data)
    {
        if (!$this->isConnected && !$this->isConnecting) {
            return;
        }

        $flowCount = count($this->receivingFlows);

        $packets = $this->parser->push($data);
        foreach ($packets as $packet) {
            $this->handlePacket($packet);
        }

        if ($flowCount > count($this->receivingFlows)) {
            $this->receivingFlows = array_values($this->receivingFlows);
        }

        $this->handleSend();
    }

    /**
     * Handles an incoming packet.
     *
     * @param Packet $packet
     */
    private function handlePacket(Packet $packet)
    {
        switch ($packet->getPacketType()) {
            case Packet::TYPE_PUBLISH:
                /* @var PublishRequestPacket $packet */
                $message = new DefaultMessage(
                    $packet->getTopic(),
                    $packet->getPayload(),
                    $packet->getQosLevel(),
                    $packet->isRetained(),
                    $packet->isDuplicate()
                );

                $this->startFlow(new IncomingPublishFlow($message, $packet->getIdentifier()));
                break;
            case Packet::TYPE_CONNACK:
            case Packet::TYPE_PINGRESP:
            case Packet::TYPE_SUBACK:
            case Packet::TYPE_UNSUBACK:
            case Packet::TYPE_PUBREL:
            case Packet::TYPE_PUBACK:
            case Packet::TYPE_PUBREC:
            case Packet::TYPE_PUBCOMP:
                $flowFound = false;
                foreach ($this->receivingFlows as $index => $flow) {
                    if ($flow->accept($packet)) {
                        $flowFound = true;

                        unset($this->receivingFlows[$index]);
                        $this->continueFlow($flow, $packet);

                        break;
                    }
                }

                if (!$flowFound) {
                    $this->emitWarning(
                        new \LogicException(sprintf('Received unexpected packet of type %d.', $packet->getPacketType()))
                    );
                }
                break;
            default:
                $this->emitWarning(
                    new \LogicException(sprintf('Cannot handle packet of type %d.', $packet->getPacketType()))
                );
        }
    }

    /**
     * Handles outgoing packets.
     */
    private function handleSend()
    {
        $flow = null;
        if ($this->writtenFlow !== null) {
            $flow = $this->writtenFlow;
            $this->writtenFlow = null;
        }

        if (count($this->sendingFlows) > 0) {
            $this->writtenFlow = array_shift($this->sendingFlows);
            $this->stream->write($this->writtenFlow->getPacket());
        }

        if ($flow !== null) {
            if ($flow->isFinished()) {
                $this->loop->nextTick(function () use ($flow) {
                    $this->finishFlow($flow);
                });
            } else {
                $this->receivingFlows[] = $flow;
            }
        }
    }

    /**
     * Handles closing of the stream.
     */
    private function handleClose()
    {
        foreach ($this->timer as $timer) {
            $this->loop->cancelTimer($timer);
        }
        $this->timer = [];

        $this->receivingFlows = [];
        $this->sendingFlows = [];

        $connection = $this->connection;

        $this->isConnecting = false;
        $this->isDisconnecting = false;
        $this->isConnected = false;
        $this->connection = null;
        $this->stream = null;

        if ($connection !== null) {
            $this->emit('close', [$connection, $this]);
        }
    }

    /**
     * Handles errors of the stream.
     *
     * @param \Exception $e
     */
    private function handleError(\Exception $e)
    {
        $this->emitError($e);
    }

    /**
     * Starts the given flow.
     *
     * @param Flow $flow
     * @param bool $isSilent
     *
     * @return ExtendedPromiseInterface
     */
    private function startFlow(Flow $flow, $isSilent = false)
    {
        try {
            $packet = $flow->start();
        } catch (\Exception $e) {
            $this->emitError($e);

            return new RejectedPromise($e);
        }

        $deferred = new Deferred();
        $internalFlow = new ReactFlow($flow, $deferred, $packet, $isSilent);

        if ($packet !== null) {
            if ($this->writtenFlow !== null) {
                $this->sendingFlows[] = $internalFlow;
            } else {
                $this->stream->write($packet);
                $this->writtenFlow = $internalFlow;
                $this->handleSend();
            }
        } else {
            $this->loop->nextTick(function () use ($internalFlow) {
                $this->finishFlow($internalFlow);
            });
        }

        return $deferred->promise();
    }

    /**
     * Continues the given flow.
     *
     * @param ReactFlow $flow
     * @param Packet    $packet
     */
    private function continueFlow(ReactFlow $flow, Packet $packet)
    {
        try {
            $response = $flow->next($packet);
        } catch (\Exception $e) {
            $this->emitError($e);

            return;
        }

        if ($response !== null) {
            if ($this->writtenFlow !== null) {
                $this->sendingFlows[] = $flow;
            } else {
                $this->stream->write($response);
                $this->writtenFlow = $flow;
                $this->handleSend();
            }
        } elseif ($flow->isFinished()) {
            $this->loop->nextTick(function () use ($flow) {
                $this->finishFlow($flow);
            });
        }
    }

    /**
     * Finishes the given flow.
     *
     * @param ReactFlow $flow
     */
    private function finishFlow(ReactFlow $flow)
    {
        if ($flow->isSuccess()) {
            if (!$flow->isSilent()) {
                $this->emit($flow->getCode(), [$flow->getResult(), $this]);
            }

            $flow->getDeferred()->resolve($flow->getResult());
        } else {
            $result = new \RuntimeException($flow->getErrorMessage());
            $this->emitWarning($result);

            $flow->getDeferred()->reject($result);
        }
    }
}
