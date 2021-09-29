<?php

declare(strict_types=1);

namespace BinSoul\Net\Mqtt\Client\React;

use BinSoul\Net\Mqtt\ClientIdentifierGenerator;
use BinSoul\Net\Mqtt\Connection;
use BinSoul\Net\Mqtt\DefaultConnection;
use BinSoul\Net\Mqtt\DefaultFlowFactory;
use BinSoul\Net\Mqtt\DefaultIdentifierGenerator;
use BinSoul\Net\Mqtt\DefaultMessage;
use BinSoul\Net\Mqtt\DefaultPacketFactory;
use BinSoul\Net\Mqtt\Flow;
use BinSoul\Net\Mqtt\FlowFactory;
use BinSoul\Net\Mqtt\Message;
use BinSoul\Net\Mqtt\Packet;
use BinSoul\Net\Mqtt\Packet\PublishRequestPacket;
use BinSoul\Net\Mqtt\StreamParser;
use BinSoul\Net\Mqtt\Subscription;
use Evenement\EventEmitter;
use LogicException;
use React\EventLoop\LoopInterface;
use React\EventLoop\TimerInterface;
use React\Promise\CancellablePromiseInterface;
use React\Promise\Deferred;
use React\Promise\ExtendedPromiseInterface;
use React\Socket\ConnectorInterface;
use React\Stream\DuplexStreamInterface;
use RuntimeException;
use Throwable;
use function React\Promise\reject;

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
    /** @var DuplexStreamInterface|null */
    private $stream;
    /** @var StreamParser */
    private $parser;
    /** @var ClientIdentifierGenerator */
    private $identifierGenerator;

    /** @var string */
    private $host;
    /** @var int */
    private $port;
    /** @var Connection|null */
    private $connection;
    /** @var bool */
    private $isConnected = false;
    /** @var bool */
    private $isConnecting = false;
    /** @var bool */
    private $isDisconnecting = false;
    /** @var callable|null */
    private $onCloseCallback;

    /** @var TimerInterface[] */
    private $timer = [];

    /** @var ReactFlow[] */
    private $receivingFlows = [];
    /** @var ReactFlow[] */
    private $sendingFlows = [];
    /** @var ReactFlow|null */
    private $writtenFlow;
    /**
     * @var FlowFactory|null
     */
    private $flowFactory;

    /**
     * Constructs an instance of this class.
     */
    public function __construct(
        ConnectorInterface $connector,
        LoopInterface $loop,
        ?ClientIdentifierGenerator $identifierGenerator = null,
        ?FlowFactory $flowFactory = null,
        ?StreamParser $parser = null
    ) {
        $this->connector = $connector;
        $this->loop = $loop;

        $this->parser = $parser;
        if ($this->parser === null) {
            $this->parser = new StreamParser(new DefaultPacketFactory());
        }

        $this->parser->onError(function (Throwable $error) {
            $this->emitWarning($error);
        });

        $this->identifierGenerator = $identifierGenerator;
        if ($this->identifierGenerator === null) {
            $this->identifierGenerator = new DefaultIdentifierGenerator();
        }

        $this->flowFactory = $flowFactory;
        if ($this->flowFactory === null) {
            $this->flowFactory = new DefaultFlowFactory($this->identifierGenerator, new DefaultIdentifierGenerator(), new DefaultPacketFactory());
        }
    }

    /**
     * Return the host.
     */
    public function getHost(): string
    {
        return $this->host;
    }

    /**
     * Return the port.
     */
    public function getPort(): int
    {
        return $this->port;
    }

    /**
     * Indicates if the client is connected.
     */
    public function isConnected(): bool
    {
        return $this->isConnected;
    }

    /**
     * Returns the underlying stream or null if the client is not connected.
     */
    public function getStream(): ?DuplexStreamInterface
    {
        return $this->stream;
    }

    /**
     * Connects to a broker.
     */
    public function connect(string $host, int $port = 1883, ?Connection $connection = null, int $timeout = 5): ExtendedPromiseInterface
    {
        if ($this->isConnected || $this->isConnecting) {
            return reject(new LogicException('The client is already connected.'));
        }

        $this->isConnecting = true;
        $this->isConnected = false;

        $this->host = $host;
        $this->port = $port;

        if ($connection === null) {
            $connection = new DefaultConnection();
        }

        if ($connection->getClientID() === '') {
            $connection = $connection->withClientID($this->identifierGenerator->generateClientIdentifier());
        }

        $deferred = new Deferred();

        $this->establishConnection($this->host, $this->port, $timeout)
            ->then(function (DuplexStreamInterface $stream) use ($connection, $deferred, $timeout) {
                $this->stream = $stream;

                $this->emit('open', [$connection, $this]);

                $this->registerClient($connection, $timeout)
                    ->then(function ($result) use ($connection, $deferred) {
                        $this->isConnecting = false;
                        $this->isConnected = true;
                        $this->connection = $connection;

                        $this->emit('connect', [$connection, $this]);
                        $deferred->resolve($result ?: $connection);
                    })
                    ->otherwise(function (Throwable $reason) use ($connection, $deferred) {
                        $this->isConnecting = false;

                        $this->emitError($reason);
                        $deferred->reject($reason);

                        if ($this->stream !== null) {
                            $this->stream->close();
                        }

                        $this->emit('close', [$connection, $this]);
                    })
                    ->done();
            })
            ->otherwise(function (Throwable $reason) use ($deferred) {
                $this->isConnecting = false;

                $this->emitError($reason);
                $deferred->reject($reason);
            })
            ->done();

        return $deferred->promise();
    }

    /**
     * Disconnects from a broker.
     */
    public function disconnect(int $timeout = 5): ExtendedPromiseInterface
    {
        if (!$this->isConnected || $this->isDisconnecting) {
            return reject(new LogicException('The client is not connected.'));
        }

        $this->isDisconnecting = true;

        $deferred = new Deferred();

        $isResolved = false;
        /** @var mixed $flowResult */
        $flowResult = null;

        $this->onCloseCallback = function ($connection) use ($deferred, &$isResolved, &$flowResult) {
            if (!$isResolved) {
                $isResolved = true;

                if ($connection) {
                    $this->emit('disconnect', [$connection, $this]);
                }

                $deferred->resolve($flowResult ?: $connection);
            }
        };

        $this->startFlow($this->flowFactory->buildOutgoingDisconnectFlow($this->connection), true)
            ->then(function ($result) use ($timeout, &$flowResult) {
                $flowResult = $result;

                $this->timer[] = $this->loop->addTimer(
                    $timeout,
                    function () {
                        if ($this->stream !== null) {
                            $this->stream->close();
                        }
                    }
                );
            })
            ->otherwise(function ($exception) use ($deferred, &$isResolved) {
                if (!$isResolved) {
                    $isResolved = true;
                    $this->isDisconnecting = false;
                    $deferred->reject($exception);
                }
            })
            ->done();

        return $deferred->promise();
    }

    /**
     * Subscribes to a topic filter.
     */
    public function subscribe(Subscription $subscription): ExtendedPromiseInterface
    {
        if (!$this->isConnected) {
            return reject(new LogicException('The client is not connected.'));
        }

        return $this->startFlow($this->flowFactory->buildOutgoingSubscribeFlow([$subscription]));
    }

    /**
     * Unsubscribes from a topic filter.
     */
    public function unsubscribe(Subscription $subscription): ExtendedPromiseInterface
    {
        if (!$this->isConnected) {
            return reject(new LogicException('The client is not connected.'));
        }

        $deferred = new Deferred();

        $this->startFlow($this->flowFactory->buildOutgoingUnsubscribeFlow([$subscription]))
            ->then(static function (array $subscriptions) use ($deferred) {
                $deferred->resolve(array_shift($subscriptions));
            })
            ->otherwise(static function ($exception) use ($deferred) {
                $deferred->reject($exception);
            })
            ->done();

        return $deferred->promise();
    }

    /**
     * Publishes a message.
     */
    public function publish(Message $message): ExtendedPromiseInterface
    {
        if (!$this->isConnected) {
            return reject(new LogicException('The client is not connected.'));
        }

        return $this->startFlow($this->flowFactory->buildOutgoingPublishFlow($message));
    }

    /**
     * Calls the given generator periodically and publishes the return value.
     */
    public function publishPeriodically(int $interval, Message $message, callable $generator): ExtendedPromiseInterface
    {
        if (!$this->isConnected) {
            return reject(new LogicException('The client is not connected.'));
        }

        $deferred = new Deferred();

        $this->timer[] = $this->loop->addPeriodicTimer(
            $interval,
            function () use ($message, $generator, $deferred) {
                $this->publish($message->withPayload((string) $generator($message->getTopic())))->done(
                    static function ($value) use ($deferred) {
                        $deferred->notify($value);
                    },
                    static function (Throwable $reason) use ($deferred) {
                        $deferred->reject($reason);
                    }
                );
            }
        );

        return $deferred->promise();
    }

    /**
     * Emits warnings.
     */
    private function emitWarning(Throwable $error): void
    {
        $this->emit('warning', [$error, $this]);
    }

    /**
     * Emits errors.
     */
    private function emitError(Throwable $error): void
    {
        $this->emit('error', [$error, $this]);
    }

    /**
     * Establishes a network connection to a server.
     */
    private function establishConnection(string $host, int $port, int $timeout): ExtendedPromiseInterface
    {
        $deferred = new Deferred();

        $future = null;
        $timer = $this->loop->addTimer(
            $timeout,
            static function () use ($deferred, $timeout, &$future) {
                $exception = new RuntimeException(sprintf('Connection timed out after %d seconds.', $timeout));
                $deferred->reject($exception);
                if ($future instanceof CancellablePromiseInterface) {
                    $future->cancel();
                }
                $future = null;
            }
        );

        $future = $this->connector->connect($host.':'.$port)
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

                $stream->on('error', function (Throwable $error) {
                    $this->handleError($error);
                });

                $deferred->resolve($stream);
            })
            ->otherwise(static function (Throwable $reason) use ($deferred) {
                $deferred->reject($reason);
            })
            ->done();

        return $deferred->promise();
    }

    /**
     * Registers a new client with the broker.
     */
    private function registerClient(Connection $connection, int $timeout): ExtendedPromiseInterface
    {
        $deferred = new Deferred();

        $responseTimer = $this->loop->addTimer(
            $timeout,
            static function () use ($deferred, $timeout) {
                $exception = new RuntimeException(sprintf('No response after %d seconds.', $timeout));
                $deferred->reject($exception);
            }
        );

        $this->startFlow($this->flowFactory->buildOutgoingConnectFlow($connection), true)
            ->always(function () use ($responseTimer) {
                $this->loop->cancelTimer($responseTimer);
            })->then(function ($result) use ($connection, $deferred) {
                $this->timer[] = $this->loop->addPeriodicTimer(
                    floor($connection->getKeepAlive() * 0.75),
                    function () {
                        $this->startFlow($this->flowFactory->buildOutgoingPingFlow());
                    }
                );

                $deferred->resolve($result ?: $connection);
            })->otherwise(static function (Throwable $reason) use ($deferred) {
                $deferred->reject($reason);
            })
            ->done();

        return $deferred->promise();
    }

    /**
     * Handles incoming data.
     */
    private function handleReceive(string $data): void
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
     */
    private function handlePacket(Packet $packet): void
    {
        switch ($packet->getPacketType()) {
            case Packet::TYPE_PUBLISH:
                if (!($packet instanceof PublishRequestPacket)) {
                    throw new RuntimeException(sprintf('Expected %s but got %s.', PublishRequestPacket::class, get_class($packet)));
                }

                $message = new DefaultMessage(
                    $packet->getTopic(),
                    $packet->getPayload(),
                    $packet->getQosLevel(),
                    $packet->isRetained(),
                    $packet->isDuplicate()
                );

                $this->startFlow($this->flowFactory->buildIncomingPublishFlow($message, $packet->getIdentifier()));
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
                        new LogicException(sprintf('Received unexpected packet of type %d.', $packet->getPacketType()))
                    );
                }
                break;
            default:
                $this->emitWarning(
                    new LogicException(sprintf('Cannot handle packet of type %d.', $packet->getPacketType()))
                );
        }
    }

    /**
     * Handles outgoing packets.
     */
    private function handleSend(): void
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
                $this->loop->futureTick(function () use ($flow) {
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
    private function handleClose(): void
    {
        foreach ($this->timer as $timer) {
            $this->loop->cancelTimer($timer);
        }

        $connection = $this->connection;

        $this->isConnecting = false;
        $this->isDisconnecting = false;
        $this->isConnected = false;
        $this->connection = null;
        $this->stream = null;

        if ($this->onCloseCallback !== null) {
            call_user_func($this->onCloseCallback, $connection);
            $this->onCloseCallback = null;
        }

        if ($connection !== null) {
            $this->emit('close', [$connection, $this]);
        }
    }

    /**
     * Handles errors of the stream.
     */
    private function handleError(Throwable $error): void
    {
        $this->emitError($error);
    }

    /**
     * Starts the given flow.
     */
    private function startFlow(Flow $flow, bool $isSilent = false): ExtendedPromiseInterface
    {
        try {
            $packet = $flow->start();
        } catch (Throwable $t) {
            $this->emitError($t);

            return reject($t);
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
            $this->loop->futureTick(function () use ($internalFlow) {
                $this->finishFlow($internalFlow);
            });
        }

        return $deferred->promise();
    }

    /**
     * Continues the given flow.
     */
    private function continueFlow(ReactFlow $flow, Packet $packet): void
    {
        try {
            $response = $flow->next($packet);
        } catch (Throwable $t) {
            $this->emitError($t);

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
            $this->loop->futureTick(function () use ($flow) {
                $this->finishFlow($flow);
            });
        }
    }

    /**
     * Finishes the given flow.
     */
    private function finishFlow(ReactFlow $flow): void
    {
        if ($flow->isSuccess()) {
            if (!$flow->isSilent()) {
                $this->emit($flow->getCode(), [$flow->getResult(), $this]);
            }

            $flow->getDeferred()->resolve($flow->getResult());
        } else {
            $result = new RuntimeException($flow->getErrorMessage());
            $this->emitWarning($result);

            $flow->getDeferred()->reject($result);
        }
    }
}
