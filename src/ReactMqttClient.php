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
use React\Promise\Deferred;
use React\Promise\PromiseInterface;
use function React\Promise\reject;
use function React\Promise\resolve;
use React\Socket\ConnectorInterface;
use React\Stream\DuplexStreamInterface;
use RuntimeException;
use Throwable;

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
    private ConnectorInterface $connector;

    private LoopInterface $loop;

    private ?DuplexStreamInterface $stream = null;

    private StreamParser $parser;

    private ClientIdentifierGenerator $identifierGenerator;

    private string $host = '';

    private int $port = 1883;

    private ?Connection $connection = null;

    private bool $isConnected = false;

    private bool $isConnecting = false;

    /**
     * @var Deferred<Connection|null>|null
     */
    private ?Deferred $connectionDeferred = null;

    private bool $isDisconnecting = false;

    /**
     * @var Deferred<Connection|null>|null
     */
    private ?Deferred $disconnectionDeferred = null;

    /**
     * @var callable|null
     */
    private $onCloseCallback = null;

    /**
     * @var TimerInterface[]
     */
    private array $timer = [];

    /**
     * @var ReactFlow[]
     */
    private array $receivingFlows = [];

    /**
     * @var ReactFlow[]
     */
    private array $sendingFlows = [];

    private ?ReactFlow $writtenFlow = null;

    private FlowFactory $flowFactory;

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
        $this->parser = $parser ?? new StreamParser(new DefaultPacketFactory());

        $this->parser->onError(
            function (Throwable $error): void {
                $this->emitWarning($error);
            }
        );

        $this->identifierGenerator = $identifierGenerator ?? new DefaultIdentifierGenerator();
        $this->flowFactory = $flowFactory ?? new DefaultFlowFactory($this->identifierGenerator, new DefaultIdentifierGenerator(), new DefaultPacketFactory());
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
     *
     * @return PromiseInterface<Connection|null>
     */
    public function connect(string $host, int $port = 1883, ?Connection $connection = null, int $timeout = 5): PromiseInterface
    {
        if ($this->isConnected) {
            return resolve($this->connection);
        }

        if ($this->isConnecting) {
            if ($this->connectionDeferred !== null) {
                return $this->connectionDeferred->promise();
            }

            return reject(new LogicException('The client is already connecting.'));
        }

        $this->isConnecting = true;
        /** @var Deferred<Connection|null> $deferred */
        $deferred = new Deferred();
        $this->connectionDeferred = $deferred;

        $this->isConnected = false;
        $this->host = $host;
        $this->port = $port;

        if ($connection === null) {
            $connection = new DefaultConnection();
        }

        if ($connection->getClientID() === '') {
            $connection = $connection->withClientID($this->identifierGenerator->generateClientIdentifier());
        }

        $this->establishConnection($this->host, $this->port, $timeout)
            ->then(
                function (DuplexStreamInterface $stream) use ($connection, $deferred, $timeout): void {
                    $this->stream = $stream;

                    $this->emit('open', [$connection, $this]);

                    $this->registerClient($connection, $timeout)
                        ->then(
                            function (?Connection $result) use ($connection, $deferred): void {
                                $this->isConnecting = false;
                                $this->connectionDeferred = null;
                                $this->isConnected = true;
                                $this->connection = $connection;

                                $this->emit('connect', [$connection, $this]);
                                $deferred->resolve($result ?: $connection);
                            }
                        )
                        ->catch(
                            function (Throwable $reason) use ($connection, $deferred): void {
                                $this->isConnecting = false;
                                $this->connectionDeferred = null;

                                $this->emitError($reason);
                                $deferred->reject($reason);

                                if ($this->stream !== null) {
                                    $this->stream->close();
                                }

                                $this->emit('close', [$connection, $this]);
                            }
                        );
                }
            )
            ->catch(
                function (Throwable $reason) use ($deferred): void {
                    $this->isConnecting = false;
                    $this->connectionDeferred = null;

                    $this->emitError($reason);
                    $deferred->reject($reason);
                }
            );

        return $deferred->promise();
    }

    /**
     * Disconnects from a broker.
     *
     * @return PromiseInterface<Connection|null>
     */
    public function disconnect(int $timeout = 5): PromiseInterface
    {
        if (! $this->isConnected || $this->connection === null) {
            $this->isConnected = false;

            return resolve($this->connection);
        }

        if ($this->isDisconnecting) {
            if ($this->disconnectionDeferred !== null) {
                return $this->disconnectionDeferred->promise();
            }

            return reject(new LogicException('The client is already disconnecting.'));
        }

        $this->isDisconnecting = true;
        /** @var Deferred<Connection|null> $deferred */
        $deferred = new Deferred();
        $this->disconnectionDeferred = $deferred;

        $isResolved = false;
        /** @var Connection|null $flowResult */
        $flowResult = null;

        $this->onCloseCallback = function (?Connection $connection) use ($deferred, &$isResolved, &$flowResult): void {
            if (! $isResolved) {
                $isResolved = true;

                if ($connection !== null) {
                    $this->emit('disconnect', [$connection, $this]);
                }

                $deferred->resolve($flowResult ?: $connection);
            }
        };

        /** @var PromiseInterface<Connection|null> $promise */
        $promise = $this->startFlow($this->flowFactory->buildOutgoingDisconnectFlow($this->connection), true);
        $promise
            ->then(
                function (?Connection $result) use ($timeout, &$flowResult): void {
                    $flowResult = $result;

                    $this->timer[] = $this->loop->addTimer(
                        $timeout,
                        function (): void {
                            if ($this->stream !== null) {
                                $this->stream->close();
                            }
                        }
                    );
                }
            )
            ->catch(
                function (Throwable $exception) use ($deferred, &$isResolved): void {
                    if (! $isResolved) {
                        $isResolved = true;
                        $this->isDisconnecting = false;
                        $this->disconnectionDeferred = null;
                        $deferred->reject($exception);
                    }
                }
            );

        return $deferred->promise();
    }

    /**
     * Subscribes to a topic filter.
     *
     * @return PromiseInterface<Subscription>
     */
    public function subscribe(Subscription $subscription): PromiseInterface
    {
        if (! $this->isConnected) {
            return reject(new LogicException('The client is not connected.'));
        }

        /** @var PromiseInterface<Subscription> $promise */
        $promise = $this->startFlow($this->flowFactory->buildOutgoingSubscribeFlow([$subscription]));

        return $promise;
    }

    /**
     * Unsubscribes from a topic filter.
     *
     * @return PromiseInterface<Subscription>
     */
    public function unsubscribe(Subscription $subscription): PromiseInterface
    {
        if (! $this->isConnected) {
            return reject(new LogicException('The client is not connected.'));
        }

        /** @var Deferred<Subscription> $deferred */
        $deferred = new Deferred();

        /** @var PromiseInterface<array<int, Subscription>> $promise */
        $promise = $this->startFlow($this->flowFactory->buildOutgoingUnsubscribeFlow([$subscription]));
        $promise
            ->then(
                static function (array $subscriptions) use ($deferred): void {
                    $subscription = array_shift($subscriptions);

                    if ($subscription instanceof Subscription) {
                        $deferred->resolve($subscription);
                    }
                }
            )
            ->catch(
                static function (Throwable $exception) use ($deferred): void {
                    $deferred->reject($exception);
                }
            );

        return $deferred->promise();
    }

    /**
     * Publishes a message.
     *
     * @return PromiseInterface<Message>
     */
    public function publish(Message $message): PromiseInterface
    {
        if (! $this->isConnected) {
            return reject(new LogicException('The client is not connected.'));
        }

        /** @var PromiseInterface<Message> $promise */
        $promise = $this->startFlow($this->flowFactory->buildOutgoingPublishFlow($message));

        return $promise;
    }

    /**
     * Calls the given generator periodically and publishes the return value.
     *
     * @param callable(string): (string|int|bool|null) $generator
     * @param (callable(Message): void)|null           $onProgress
     *
     * @return PromiseInterface<never>
     */
    public function publishPeriodically(int $interval, Message $message, callable $generator, callable $onProgress = null): PromiseInterface
    {
        if (! $this->isConnected) {
            return reject(new LogicException('The client is not connected.'));
        }

        /** @var Deferred<never> $deferred */
        $deferred = new Deferred();

        $this->timer[] = $this->loop->addPeriodicTimer(
            $interval,
            function () use ($message, $generator, $onProgress, $deferred): void {
                $this->publish($message->withPayload((string) $generator($message->getTopic())))
                    ->then(
                        static function (Message $value) use ($onProgress): void {
                            if ($onProgress !== null) {
                                $onProgress($value);
                            }
                        },
                        static function (Throwable $reason) use ($deferred): void {
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
     *
     * @return PromiseInterface<DuplexStreamInterface>
     */
    private function establishConnection(string $host, int $port, int $timeout): PromiseInterface
    {
        /** @var Deferred<DuplexStreamInterface> $deferred */
        $deferred = new Deferred();

        /** @var PromiseInterface<ConnectorInterface>|null $future */
        $future = null;

        $timer = $this->loop->addTimer(
            $timeout,
            static function () use ($deferred, $timeout, &$future): void {
                $exception = new RuntimeException(sprintf('Connection timed out after %d seconds.', $timeout));
                $deferred->reject($exception);

                if ($future instanceof PromiseInterface) {
                    $future->cancel();
                }

                $future = null;
            }
        );

        $future = $this->connector->connect($host . ':' . $port)
            ->then(
                function (DuplexStreamInterface $stream) use ($deferred): void {
                    $stream->on(
                        'data',
                        function (string $data): void {
                            $this->handleReceive($data);
                        }
                    );

                    $stream->on(
                        'close',
                        function (): void {
                            $this->handleClose();
                        }
                    );

                    $stream->on(
                        'error',
                        function (Throwable $error): void {
                            $this->handleError($error);
                        }
                    );

                    $deferred->resolve($stream);
                }
            )
            ->catch(
                static function (Throwable $reason) use ($deferred): void {
                    $deferred->reject($reason);
                }
            )
            ->finally(
                function () use ($timer): void {
                    $this->loop->cancelTimer($timer);
                }
            );

        return $deferred->promise();
    }

    /**
     * Registers a new client with the broker.
     *
     * @return PromiseInterface<Connection|null>
     */
    private function registerClient(Connection $connection, int $timeout): PromiseInterface
    {
        /** @var Deferred<Connection|null> $deferred */
        $deferred = new Deferred();

        $responseTimer = $this->loop->addTimer(
            $timeout,
            static function () use ($deferred, $timeout): void {
                $exception = new RuntimeException(sprintf('No response after %d seconds.', $timeout));
                $deferred->reject($exception);
            }
        );

        /** @var PromiseInterface<Connection|null> $promise */
        $promise = $this->startFlow($this->flowFactory->buildOutgoingConnectFlow($connection), true);
        $promise
            ->then(
                function (?Connection $result) use ($connection, $deferred): void {
                    $this->timer[] = $this->loop->addPeriodicTimer(
                        floor($connection->getKeepAlive() * 0.75),
                        function (): void {
                            $this->startFlow($this->flowFactory->buildOutgoingPingFlow());
                        }
                    );

                    $deferred->resolve($result ?: $connection);
                }
            )
            ->catch(
                static function (Throwable $reason) use ($deferred): void {
                    $deferred->reject($reason);
                }
            )
            ->finally(
                function () use ($responseTimer): void {
                    $this->loop->cancelTimer($responseTimer);
                }
            );

        return $deferred->promise();
    }

    /**
     * Handles incoming data.
     */
    private function handleReceive(string $data): void
    {
        if (! $this->isConnected && ! $this->isConnecting) {
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
                if (! ($packet instanceof PublishRequestPacket)) {
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

                if (! $flowFound) {
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

        if ($this->sendingFlows !== []) {
            $this->writtenFlow = array_shift($this->sendingFlows);

            if ($this->writtenFlow !== null && $this->stream !== null) {
                $this->stream->write($this->writtenFlow->getPacket());
            }
        }

        if ($flow !== null) {
            if ($flow->isFinished()) {
                $this->loop->futureTick(
                    function () use ($flow): void {
                        $this->finishFlow($flow);
                    }
                );
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
        $this->connectionDeferred = null;
        $this->isDisconnecting = false;
        $this->disconnectionDeferred = null;
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
     *
     * @return PromiseInterface<mixed>
     */
    private function startFlow(Flow $flow, bool $isSilent = false): PromiseInterface
    {
        try {
            $packet = $flow->start();
        } catch (Throwable $throwable) {
            $this->emitError($throwable);

            return reject($throwable);
        }

        $deferred = new Deferred();
        $internalFlow = new ReactFlow($flow, $deferred, $packet, $isSilent);

        if ($packet !== null) {
            if ($this->writtenFlow !== null) {
                $this->sendingFlows[] = $internalFlow;
            } else {
                if ($this->stream !== null) {
                    $this->stream->write($packet);
                }

                $this->writtenFlow = $internalFlow;
                $this->handleSend();
            }
        } else {
            $this->loop->futureTick(
                function () use ($internalFlow): void {
                    $this->finishFlow($internalFlow);
                }
            );
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
        } catch (Throwable $throwable) {
            $this->emitError($throwable);

            return;
        }

        if ($response !== null) {
            if ($this->writtenFlow !== null) {
                $this->sendingFlows[] = $flow;
            } else {
                if ($this->stream !== null) {
                    $this->stream->write($response);
                }

                $this->writtenFlow = $flow;
                $this->handleSend();
            }
        } elseif ($flow->isFinished()) {
            $this->loop->futureTick(
                function () use ($flow): void {
                    $this->finishFlow($flow);
                }
            );
        }
    }

    /**
     * Finishes the given flow.
     */
    private function finishFlow(ReactFlow $flow): void
    {
        if ($flow->isSuccess()) {
            if (! $flow->isSilent()) {
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
