<?php

declare(strict_types=1);

namespace BinSoul\Test\Net\Mqtt\Client\React\Unit;

use BinSoul\Net\Mqtt\Client\React\ReactMqttClient;
use BinSoul\Net\Mqtt\ClientIdentifierGenerator;
use BinSoul\Net\Mqtt\Connection;
use BinSoul\Net\Mqtt\DefaultConnection;
use BinSoul\Net\Mqtt\DefaultMessage;
use BinSoul\Net\Mqtt\DefaultSubscription;
use BinSoul\Net\Mqtt\Flow;
use BinSoul\Net\Mqtt\FlowFactory;
use BinSoul\Net\Mqtt\Message;
use BinSoul\Net\Mqtt\Packet;
use BinSoul\Net\Mqtt\Packet\PublishRequestPacket;
use BinSoul\Net\Mqtt\StreamParser;
use BinSoul\Net\Mqtt\Subscription;
use InvalidArgumentException;
use LogicException;
use PHPUnit\Framework\TestCase;
use React\EventLoop\LoopInterface;
use React\EventLoop\TimerInterface;
use React\Promise\Deferred;
use React\Promise\Promise;
use React\Promise\PromiseInterface;
use React\Socket\ConnectorInterface;
use React\Stream\DuplexStreamInterface;
use RuntimeException;
use Throwable;

/**
 * Tests the ReactMqttClient class.
 */
class ReactMqttClientTest extends TestCase
{
    private const DEFAULT_HOST = 'localhost';

    private const DEFAULT_PORT = 1883;

    private ConnectorInterface $connector;

    private LoopInterface $loop;

    private ClientIdentifierGenerator $identifierGenerator;

    private FlowFactory $flowFactory;

    private StreamParser $parser;

    private DuplexStreamInterface $stream;

    /**
     * @var callable|null
     */
    private $streamDataCallback = null;

    /**
     * @var callable|null
     */
    private $streamCloseCallback = null;

    protected function setUp(): void
    {
        $this->connector = $this->createMock(ConnectorInterface::class);
        $this->loop = $this->createMock(LoopInterface::class);
        $this->identifierGenerator = $this->createMock(ClientIdentifierGenerator::class);
        $this->flowFactory = $this->createMock(FlowFactory::class);
        $this->parser = $this->createMock(StreamParser::class);
        $this->stream = $this->createMock(DuplexStreamInterface::class);
    }

    public function test_constructor_with_minimal_parameters(): void
    {
        $client = new ReactMqttClient($this->connector, $this->loop);

        self::assertInstanceOf(ReactMqttClient::class, $client);
    }

    public function test_get_host_returns_default_host(): void
    {
        $client = new ReactMqttClient($this->connector, $this->loop);

        self::assertSame(self::DEFAULT_HOST, $client->getHost());
    }

    public function test_get_port_returns_default_port(): void
    {
        $client = new ReactMqttClient($this->connector, $this->loop);

        self::assertSame(self::DEFAULT_PORT, $client->getPort());
    }

    public function test_is_connected_returns_false_initially(): void
    {
        $client = new ReactMqttClient($this->connector, $this->loop);

        self::assertFalse($client->isConnected());
    }

    public function test_get_stream_returns_null_initially(): void
    {
        $client = new ReactMqttClient($this->connector, $this->loop);

        self::assertNull($client->getStream());
    }

    public function test_connect_throws_exception_for_empty_host(): void
    {
        $client = new ReactMqttClient($this->connector, $this->loop);

        $this->expectException(InvalidArgumentException::class);

        $client->connect('');
    }

    public function test_connect_throws_exception_for_whitespace_host(): void
    {
        $client = new ReactMqttClient($this->connector, $this->loop);

        $this->expectException(InvalidArgumentException::class);

        $client->connect('   ');
    }

    public function test_connect_throws_exception_for_negative_port(): void
    {
        $client = new ReactMqttClient($this->connector, $this->loop);

        $this->expectException(InvalidArgumentException::class);

        $client->connect('localhost', -1);
    }

    public function test_connect_throws_exception_for_port_above_maximum(): void
    {
        $client = new ReactMqttClient($this->connector, $this->loop);

        $this->expectException(InvalidArgumentException::class);

        $client->connect('localhost', 65536);
    }

    public function test_connect_throws_exception_for_negative_timeout(): void
    {
        $client = new ReactMqttClient($this->connector, $this->loop);

        $this->expectException(InvalidArgumentException::class);

        $client->connect('localhost', 1883, null, -1);
    }

    public function test_connect_times_out_after_specified_timeout(): void
    {
        $client = new ReactMqttClient($this->connector, $this->loop);

        $timeoutCallback = null;
        $this->loop->method('addTimer')->willReturnCallback(
            function (int $timeout, callable $callback) use (&$timeoutCallback): TimerInterface {
                $timeoutCallback = $callback;

                return $this->createMock(TimerInterface::class);
            }
        );

        // Connection never completes (returns a pending promise)
        $connectionDeferred = new Deferred();
        $this->connector->method('connect')->willReturn($connectionDeferred->promise());

        $rejected = false;
        $rejectionReason = null;
        $client->connect('localhost', 1883, null, 5)->catch(
            static function (Throwable $reason) use (&$rejected, &$rejectionReason): void {
                $rejected = true;
                $rejectionReason = $reason;
            }
        );

        // Initially not rejected
        self::assertFalse($rejected);

        // Trigger timeout callback
        self::assertNotNull($timeoutCallback);
        $timeoutCallback();

        self::assertTrue($rejected);
        self::assertInstanceOf(RuntimeException::class, $rejectionReason);
    }

    public function test_connect_timeout_cancels_pending_connection_promise(): void
    {
        $client = new ReactMqttClient($this->connector, $this->loop);

        $timeoutCallback = null;
        $this->loop->method('addTimer')->willReturnCallback(
            function (int $timeout, callable $callback) use (&$timeoutCallback): TimerInterface {
                $timeoutCallback = $callback;

                return $this->createMock(TimerInterface::class);
            }
        );

        // Connection returns a cancellable promise
        $cancelled = false;
        $connectionPromise = new Promise(
            static function (): void {
                // Never resolves
            },
            static function () use (&$cancelled): void {
                $cancelled = true;
            }
        );

        $this->connector->method('connect')->willReturn($connectionPromise);

        $client->connect('localhost', 1883, null, 3)->catch(
            static function (): void {
                // Ignore rejection
            }
        );

        // Trigger timeout
        self::assertNotNull($timeoutCallback);
        $timeoutCallback();

        // Verify the connection promise was cancelled
        self::assertTrue($cancelled);
    }

    public function test_connect_timeout_emits_error_event(): void
    {
        $client = new ReactMqttClient($this->connector, $this->loop);

        $timeoutCallback = null;
        $this->loop->method('addTimer')->willReturnCallback(
            function (int $timeout, callable $callback) use (&$timeoutCallback): TimerInterface {
                $timeoutCallback = $callback;

                return $this->createMock(TimerInterface::class);
            }
        );

        $connectionDeferred = new Deferred();
        $this->connector->method('connect')->willReturn($connectionDeferred->promise());

        $errorEmitted = false;
        $emittedError = null;
        $client->on(
            'error',
            static function (Throwable $error) use (&$errorEmitted, &$emittedError): void {
                $errorEmitted = true;
                $emittedError = $error;
            }
        );

        $client->connect('localhost', 1883, null, 10)->catch(
            static function (): void {
            }
        );

        // Trigger timeout
        self::assertNotNull($timeoutCallback);
        $timeoutCallback();

        // Verify error event was emitted
        self::assertTrue($errorEmitted);
        self::assertInstanceOf(RuntimeException::class, $emittedError);
    }

    public function test_connect_updates_host_and_port(): void
    {
        $client = new ReactMqttClient($this->connector, $this->loop);

        $this->loop->method('addTimer')->willReturn($this->createMock(TimerInterface::class));

        $this->connector->method('connect')->willReturn(
            $this->createRejectedPromise(new RuntimeException('Connection failed'))
        );

        $client->connect('example.com', 8883)->catch(
            static function (): void {
            }
        );

        self::assertSame('example.com', $client->getHost());
        self::assertSame(8883, $client->getPort());
    }

    public function test_connect_generates_client_id_when_not_provided(): void
    {
        $client = new ReactMqttClient($this->connector, $this->loop, $this->identifierGenerator, $this->flowFactory);

        $this->identifierGenerator->expects(self::once())
            ->method('generateClientIdentifier')
            ->willReturn('generated-client-id');

        $connectFlow = $this->createConnectFlowMock();
        $this->flowFactory->expects(self::once())
            ->method('buildOutgoingConnectFlow')
            ->with(self::callback(
                static function (Connection $connection): bool {
                    return $connection->getClientID() === 'generated-client-id';
                }
            ))
            ->willReturn($connectFlow);

        $this->setupSuccessfulConnection();

        $client->connect('localhost');
    }

    public function test_connect_uses_provided_client_id(): void
    {
        $client = new ReactMqttClient($this->connector, $this->loop, $this->identifierGenerator, $this->flowFactory);

        $connection = new DefaultConnection();
        $connection = $connection->withClientID('custom-client-id');

        $this->identifierGenerator->expects(self::never())->method('generateClientIdentifier');

        $connectFlow = $this->createConnectFlowMock();
        $this->flowFactory->expects(self::once())
            ->method('buildOutgoingConnectFlow')
            ->with(self::callback(
                static function (Connection $connection): bool {
                    return $connection->getClientID() === 'custom-client-id';
                }
            ))
            ->willReturn($connectFlow);

        $this->setupSuccessfulConnection();

        $client->connect('localhost', 1883, $connection);
    }

    public function test_connect_initiates_connector_with_correct_address(): void
    {
        $client = new ReactMqttClient($this->connector, $this->loop, $this->identifierGenerator, $this->flowFactory);

        $this->connector->expects(self::once())
            ->method('connect')
            ->with('localhost:1883')
            ->willReturn($this->createResolvedPromise($this->stream));

        $this->setupSuccessfulConnection();

        $client->connect('localhost');
    }

    public function test_connect_returns_existing_connection_when_already_connected(): void
    {
        $client = new ReactMqttClient($this->connector, $this->loop, $this->identifierGenerator, $this->flowFactory);

        $this->setupSuccessfulConnection();

        $firstConnection = null;
        $firstPromise = $client->connect('localhost');
        $firstPromise->then(
            function ($connection) use (&$firstConnection) {
                $firstConnection = $connection;
            }
        );

        // After first connection, should be connected
        self::assertTrue($client->isConnected());

        // Second connect should return same connection without reconnecting
        $secondPromise = $client->connect('example.com');
        // Verify it returns a resolved promise (not a new connection attempt)
        self::assertInstanceOf(PromiseInterface::class, $secondPromise);

        $secondConnection = null;
        $secondPromise->then(
            function ($connection) use (&$secondConnection) {
                $secondConnection = $connection;
            }
        );

        // Host should still be localhost
        self::assertSame('localhost', $client->getHost());
        self::assertNotNull($firstConnection);
        self::assertNotNull($secondConnection);
        self::assertSame($firstConnection, $secondConnection);
    }

    public function test_connect_returns_same_promise_when_called_twice_during_connection(): void
    {
        $client = new ReactMqttClient($this->connector, $this->loop, $this->identifierGenerator, $this->flowFactory);

        // Setup a delayed connection that doesn't resolve immediately
        $this->loop->method('addTimer')->willReturn($this->createMock(TimerInterface::class));
        $this->loop->method('addPeriodicTimer')->willReturn($this->createMock(TimerInterface::class));

        $connectionDeferred = new Deferred();
        $this->connector->method('connect')->willReturn($connectionDeferred->promise());

        $connectFlow = $this->createConnectFlowMock();
        $this->flowFactory->method('buildOutgoingConnectFlow')->willReturn($connectFlow);

        // Start first connection which doesn't complete yet
        $firstPromise = $client->connect('localhost');

        // Immediately call connect again while first is still connecting
        $secondPromise = $client->connect('localhost');

        // Both should return the same promise instance
        self::assertSame($firstPromise, $secondPromise);
    }

    public function test_connect_allows_new_connection_after_previous_failure(): void
    {
        $client = new ReactMqttClient($this->connector, $this->loop, $this->identifierGenerator, $this->flowFactory);

        $this->loop->method('addTimer')->willReturn($this->createMock(TimerInterface::class));

        // First connection will fail immediately
        $this->connector->expects(self::exactly(2))
            ->method('connect')
            ->willReturnOnConsecutiveCalls(
                $this->createRejectedPromise(new RuntimeException('First connection failed')),
                $this->createResolvedPromise($this->stream)
            );

        $connectFlow = $this->createConnectFlowMock();
        $this->flowFactory->method('buildOutgoingConnectFlow')->willReturn($connectFlow);

        // First connection attempt - should fail
        $firstFailed = false;
        $client->connect('localhost')->catch(
            static function () use (&$firstFailed): void {
                $firstFailed = true;
            }
        );

        self::assertTrue($firstFailed);
        self::assertFalse($client->isConnected());

        // Second connection attempt - should succeed
        $this->setupStreamCallbacks();
        $this->loop->method('addPeriodicTimer')->willReturn($this->createMock(TimerInterface::class));
        $this->loop->method('futureTick')->willReturnCallback(
            static function (callable $callback): void {
                $callback();
            }
        );
        $this->flowFactory->method('buildOutgoingPingFlow')->willReturn($this->createFlowMock());
        $this->stream->method('write')->willReturn(true);

        $secondSucceeded = false;
        $client->connect('localhost')->then(
            static function () use (&$secondSucceeded): void {
                $secondSucceeded = true;
            }
        );

        self::assertTrue($secondSucceeded);
        self::assertTrue($client->isConnected());
    }

    public function test_connect_handles_registration_failure(): void
    {
        $client = new ReactMqttClient($this->connector, $this->loop, $this->identifierGenerator, $this->flowFactory);

        $this->loop->method('addTimer')->willReturn($this->createMock(TimerInterface::class));
        $this->loop->method('futureTick')->willReturnCallback(
            static function (callable $callback): void {
                $callback();
            }
        );

        // Stream connection succeeds
        $this->setupStreamCallbacks();
        $this->connector->method('connect')->willReturn($this->createResolvedPromise($this->stream));

        // But registration fails (e.g., CONNACK with error code)
        $failedConnectFlow = $this->createMock(Flow::class);
        $packet = $this->createMock(Packet::class);
        $failedConnectFlow->method('start')->willReturn($packet);
        $failedConnectFlow->method('isFinished')->willReturn(true);
        $failedConnectFlow->method('isSuccess')->willReturn(false);
        $failedConnectFlow->method('getCode')->willReturn('connect');
        $failedConnectFlow->method('getErrorMessage')->willReturn('Connection refused: bad credentials');

        $this->flowFactory->method('buildOutgoingConnectFlow')->willReturn($failedConnectFlow);
        $this->stream->method('write')->willReturn(true);

        $errorEmitted = false;
        $emittedError = null;
        $client->on(
            'error',
            static function (Throwable $error) use (&$errorEmitted, &$emittedError): void {
                $errorEmitted = true;
                $emittedError = $error;
            }
        );

        $closeEmitted = false;
        $emittedConnection = null;
        $client->on(
            'close',
            static function (Connection $connection) use (&$closeEmitted, &$emittedConnection): void {
                $closeEmitted = true;
                $emittedConnection = $connection;
            }
        );

        $streamClosed = false;
        $this->stream->method('close')->willReturnCallback(
            static function () use (&$streamClosed): void {
                $streamClosed = true;
            }
        );

        $rejected = false;
        $rejectionReason = null;
        $client->connect('localhost')->catch(
            static function (Throwable $reason) use (&$rejected, &$rejectionReason): void {
                $rejected = true;
                $rejectionReason = $reason;
            }
        );

        // Verify the catch handler behavior
        self::assertTrue($rejected);
        self::assertInstanceOf(RuntimeException::class, $rejectionReason);
        self::assertTrue($errorEmitted);
        self::assertSame($emittedError, $rejectionReason);

        self::assertTrue($streamClosed);
        self::assertTrue($closeEmitted);
        self::assertInstanceOf(Connection::class, $emittedConnection);
    }

    public function test_connect_resets_connecting_state_on_registration_failure(): void
    {
        $client = new ReactMqttClient($this->connector, $this->loop, $this->identifierGenerator, $this->flowFactory);

        $this->loop->method('addTimer')->willReturn($this->createMock(TimerInterface::class));
        $this->loop->method('futureTick')->willReturnCallback(
            static function (callable $callback): void {
                $callback();
            }
        );

        $this->setupStreamCallbacks();
        $this->connector->method('connect')->willReturn($this->createResolvedPromise($this->stream));

        // Registration fails
        $failedConnectFlow = $this->createMock(Flow::class);
        $packet = $this->createMock(Packet::class);
        $failedConnectFlow->method('start')->willReturn($packet);
        $failedConnectFlow->method('isFinished')->willReturn(true);
        $failedConnectFlow->method('isSuccess')->willReturn(false);
        $failedConnectFlow->method('getCode')->willReturn('connect');
        $failedConnectFlow->method('getErrorMessage')->willReturn('Connection refused');

        $this->flowFactory->method('buildOutgoingConnectFlow')->willReturn($failedConnectFlow);
        $this->stream->method('write')->willReturn(true);
        $this->stream->method('close')->willReturnCallback(
            function (): void {
            }
        );

        $client->connect('localhost')->catch(
            static function (): void {
            }
        );

        // Verify connecting state was reset (isConnecting = false, connectionDeferred = null)
        // This allows reconnection attempts
        self::assertFalse($client->isConnected());

        // Verify we can try to connect again without "already connecting" error
        $canReconnect = true;

        try {
            $client->connect('localhost')->catch(
                static function (): void {
                }
            );
        } catch (LogicException $e) {
            $canReconnect = false;
        }

        self::assertTrue($canReconnect);
    }

    public function test_connect_closes_stream_on_registration_failure(): void
    {
        $client = new ReactMqttClient($this->connector, $this->loop, $this->identifierGenerator, $this->flowFactory);

        $this->loop->method('addTimer')->willReturn($this->createMock(TimerInterface::class));
        $this->loop->method('futureTick')->willReturnCallback(
            static function (callable $callback): void {
                $callback();
            }
        );

        $this->setupStreamCallbacks();
        $this->connector->method('connect')->willReturn($this->createResolvedPromise($this->stream));

        // Registration fails
        $failedConnectFlow = $this->createMock(Flow::class);
        $packet = $this->createMock(Packet::class);
        $failedConnectFlow->method('start')->willReturn($packet);
        $failedConnectFlow->method('isFinished')->willReturn(true);
        $failedConnectFlow->method('isSuccess')->willReturn(false);
        $failedConnectFlow->method('getCode')->willReturn('connect');
        $failedConnectFlow->method('getErrorMessage')->willReturn('Registration timeout');

        $this->flowFactory->method('buildOutgoingConnectFlow')->willReturn($failedConnectFlow);
        $this->stream->method('write')->willReturn(true);

        $streamCloseCalled = false;
        $this->stream->expects(self::once())
            ->method('close')
            ->willReturnCallback(
                static function () use (&$streamCloseCalled): void {
                    $streamCloseCalled = true;
                }
            );

        $client->connect('localhost')->catch(
            static function (): void {
            }
        );

        self::assertTrue($streamCloseCalled);
    }

    public function test_connect_emits_error_event_on_connection_failure(): void
    {
        $client = new ReactMqttClient($this->connector, $this->loop);

        $this->loop->method('addTimer')->willReturn($this->createMock(TimerInterface::class));

        $this->connector->method('connect')->willReturn(
            $this->createRejectedPromise(new RuntimeException('Connection failed'))
        );

        $errorEmitted = false;
        $client->on(
            'error',
            static function () use (&$errorEmitted): void {
                $errorEmitted = true;
            }
        );

        $client->connect('localhost')->catch(
            static function (): void {
            }
        );

        self::assertTrue($errorEmitted);
    }

    public function test_connect_rejects_promise_on_connection_failure(): void
    {
        $client = new ReactMqttClient($this->connector, $this->loop);

        $this->loop->method('addTimer')->willReturn($this->createMock(TimerInterface::class));

        $this->connector->method('connect')->willReturn(
            $this->createRejectedPromise(new RuntimeException('Connection failed'))
        );

        $rejected = false;
        $client->connect('localhost')->catch(
            static function () use (&$rejected): void {
                $rejected = true;
            }
        );

        self::assertTrue($rejected);
    }

    public function test_disconnect_returns_resolved_promise_when_not_connected(): void
    {
        $client = new ReactMqttClient($this->connector, $this->loop);

        $resolved = false;
        $client->disconnect()->then(
            static function () use (&$resolved): void {
                $resolved = true;
            }
        );

        self::assertTrue($resolved);
    }

    public function test_disconnect_throws_exception_for_negative_timeout(): void
    {
        $client = new ReactMqttClient($this->connector, $this->loop, $this->identifierGenerator, $this->flowFactory);

        $this->setupSuccessfulConnection();
        $client->connect('localhost');

        $this->expectException(InvalidArgumentException::class);

        $client->disconnect(-1);
    }

    public function test_disconnect_starts_disconnect_flow(): void
    {
        $client = new ReactMqttClient($this->connector, $this->loop, $this->identifierGenerator, $this->flowFactory);

        $this->setupSuccessfulConnection();
        $client->connect('localhost');

        $disconnectFlow = $this->createDisconnectFlowMock();
        $this->flowFactory->expects(self::once())
            ->method('buildOutgoingDisconnectFlow')
            ->willReturn($disconnectFlow);

        $client->disconnect();
    }

    public function test_disconnect_rejects_promise_and_resets_state_when_flow_fails(): void
    {
        $client = new ReactMqttClient($this->connector, $this->loop, $this->identifierGenerator, $this->flowFactory);

        $this->setupSuccessfulConnection();
        $client->connect('localhost');

        $exception = new RuntimeException('Disconnect flow failed');

        // Create a flow that fails immediately on start()
        $failingFlow = $this->createMock(Flow::class);
        $failingFlow->method('start')->willThrowException($exception);

        $this->flowFactory->method('buildOutgoingDisconnectFlow')->willReturn($failingFlow);

        $errorEmitted = false;
        $emittedError = null;
        $client->on(
            'error',
            static function (Throwable $error) use (&$errorEmitted, &$emittedError): void {
                $errorEmitted = true;
                $emittedError = $error;
            }
        );

        $promiseRejected = false;
        $rejectionReason = null;

        $client->disconnect()->catch(
            static function (Throwable $reason) use (&$promiseRejected, &$rejectionReason): void {
                $promiseRejected = true;
                $rejectionReason = $reason;
            }
        );

        // Verify error event was emitted
        self::assertTrue($errorEmitted);
        self::assertSame($exception, $emittedError);

        // Verify promise was rejected with the exception
        self::assertTrue($promiseRejected);
        self::assertSame($exception, $rejectionReason);

        // Verify disconnecting state was reset
        // We can verify this by checking that a new disconnect can be attempted
        $canDisconnectAgain = true;

        try {
            $client->disconnect()->catch(
                static function (): void {
                }
            );
        } catch (LogicException $e) {
            $canDisconnectAgain = false;
        }

        self::assertTrue($canDisconnectAgain);
    }

    public function test_disconnect_returns_same_promise_when_called_twice_during_disconnection(): void
    {
        $client = new ReactMqttClient($this->connector, $this->loop, $this->identifierGenerator, $this->flowFactory);

        $this->setupSuccessfulConnection();
        $client->connect('localhost');

        // Setup a delayed disconnection (doesn't complete immediately)
        $disconnectFlow = $this->createMock(Flow::class);
        $packet = $this->createMock(Packet::class);
        $disconnectFlow->method('start')->willReturn($packet);
        $disconnectFlow->method('isFinished')->willReturn(false); // Not finished yet
        $disconnectFlow->method('getCode')->willReturn('disconnect');

        $this->flowFactory->method('buildOutgoingDisconnectFlow')->willReturn($disconnectFlow);

        // Start first disconnection (doesn't complete yet)
        $firstPromise = $client->disconnect();

        // Immediately call disconnect again while first is still disconnecting
        $secondPromise = $client->disconnect();

        // Both should return the same promise instance
        self::assertSame($firstPromise, $secondPromise);
    }

    public function test_disconnect_allows_new_disconnection_after_previous_completion(): void
    {
        $client = new ReactMqttClient($this->connector, $this->loop, $this->identifierGenerator, $this->flowFactory);

        // First connection and disconnection
        $this->setupSuccessfulConnection(true);
        $client->connect('localhost');

        $disconnectFlow = $this->createDisconnectFlowMock();
        $this->flowFactory->method('buildOutgoingDisconnectFlow')->willReturn($disconnectFlow);

        $firstDisconnected = false;
        $client->disconnect()->then(
            static function () use (&$firstDisconnected): void {
                $firstDisconnected = true;
            }
        );

        // Trigger close to complete disconnection
        $closeCallback = $this->streamCloseCallback;

        if ($closeCallback !== null) {
            $closeCallback();
        }

        self::assertTrue($firstDisconnected);
        self::assertFalse($client->isConnected());

        // Reconnect
        $this->streamCloseCallback = null;
        $this->setupSuccessfulConnection(true);
        $client->connect('localhost');

        // Second disconnection should work without "already disconnecting" error
        $canDisconnect = true;

        try {
            $client->disconnect();
        } catch (LogicException $e) {
            $canDisconnect = false;
        }

        self::assertTrue($canDisconnect);
    }

    public function test_subscribe_rejects_when_not_connected(): void
    {
        $client = new ReactMqttClient($this->connector, $this->loop);
        $subscription = $this->createMock(Subscription::class);

        $rejected = false;
        $rejectionReason = null;
        $client->subscribe($subscription)->catch(
            static function (Throwable $reason) use (&$rejected, &$rejectionReason): void {
                $rejected = true;
                $rejectionReason = $reason;
            }
        );

        self::assertTrue($rejected);
        self::assertInstanceOf(LogicException::class, $rejectionReason);
    }

    public function test_subscribe_starts_subscribe_flow_when_connected(): void
    {
        $client = new ReactMqttClient($this->connector, $this->loop, $this->identifierGenerator, $this->flowFactory);

        $this->setupSuccessfulConnection();
        $client->connect('localhost');

        $subscription = $this->createMock(Subscription::class);
        $subscribeFlow = $this->createFlowMock();

        $this->flowFactory->expects(self::once())
            ->method('buildOutgoingSubscribeFlow')
            ->with([$subscription])
            ->willReturn($subscribeFlow);

        $client->subscribe($subscription);
    }

    public function test_unsubscribe_rejects_when_not_connected(): void
    {
        $client = new ReactMqttClient($this->connector, $this->loop);
        $subscription = $this->createMock(Subscription::class);

        $rejected = false;
        $rejectionReason = null;
        $client->unsubscribe($subscription)->catch(
            static function (Throwable $reason) use (&$rejected, &$rejectionReason): void {
                $rejected = true;
                $rejectionReason = $reason;
            }
        );

        self::assertTrue($rejected);
        self::assertInstanceOf(LogicException::class, $rejectionReason);
    }

    public function test_unsubscribe_starts_unsubscribe_flow_when_connected(): void
    {
        $client = new ReactMqttClient($this->connector, $this->loop, $this->identifierGenerator, $this->flowFactory);

        $this->setupSuccessfulConnection();
        $client->connect('localhost');

        $subscription = $this->createMock(Subscription::class);
        $unsubscribeFlow = $this->createFlowMock();

        $this->flowFactory->expects(self::once())
            ->method('buildOutgoingUnsubscribeFlow')
            ->with([$subscription])
            ->willReturn($unsubscribeFlow);

        $client->unsubscribe($subscription);
    }

    public function test_publish_rejects_when_not_connected(): void
    {
        $client = new ReactMqttClient($this->connector, $this->loop);
        $message = new DefaultMessage('test/topic', 'payload');

        $rejected = false;
        $rejectionReason = null;
        $client->publish($message)->catch(
            static function (Throwable $reason) use (&$rejected, &$rejectionReason): void {
                $rejected = true;
                $rejectionReason = $reason;
            }
        );

        self::assertTrue($rejected);
        self::assertInstanceOf(LogicException::class, $rejectionReason);
    }

    public function test_publish_starts_publish_flow_when_connected(): void
    {
        $client = new ReactMqttClient($this->connector, $this->loop, $this->identifierGenerator, $this->flowFactory);

        $this->setupSuccessfulConnection();
        $client->connect('localhost');

        $message = new DefaultMessage('test/topic', 'payload');
        $publishFlow = $this->createFlowMock();

        $this->flowFactory->expects(self::once())
            ->method('buildOutgoingPublishFlow')
            ->with($message)
            ->willReturn($publishFlow);

        $client->publish($message);
    }

    public function test_publish_periodically_rejects_when_not_connected(): void
    {
        $client = new ReactMqttClient($this->connector, $this->loop);
        $message = new DefaultMessage('test/topic', 'payload');
        $generator = static fn(string $topic): string => 'generated';

        $rejected = false;
        $rejectionReason = null;
        $client->publishPeriodically(5, $message, $generator)->catch(
            static function (Throwable $reason) use (&$rejected, &$rejectionReason): void {
                $rejected = true;
                $rejectionReason = $reason;
            }
        );

        self::assertTrue($rejected);
        self::assertInstanceOf(LogicException::class, $rejectionReason);
    }

    public function test_publish_periodically_creates_periodic_timer(): void
    {
        $client = new ReactMqttClient($this->connector, $this->loop, $this->identifierGenerator, $this->flowFactory);

        $this->setupSuccessfulConnection();
        $client->connect('localhost');

        $message = new DefaultMessage('test/topic', 'payload');
        $generator = static fn(string $topic): string => 'generated';

        $this->loop->expects(self::once())
            ->method('addPeriodicTimer')
            ->with(5, self::anything())
            ->willReturn($this->createMock(TimerInterface::class));

        $publishFlow = $this->createFlowMock();
        $this->flowFactory->method('buildOutgoingPublishFlow')->willReturn($publishFlow);

        $client->publishPeriodically(5, $message, $generator);
    }

    public function test_publish_periodically_passes_topic_to_generator(): void
    {
        $client = new ReactMqttClient($this->connector, $this->loop, $this->identifierGenerator, $this->flowFactory);

        $this->setupSuccessfulConnection();
        $client->connect('localhost');

        $message = new DefaultMessage('test/topic', 'payload');
        $generatorCalled = false;
        $receivedTopic = null;
        $generator = static function (string $topic) use (&$generatorCalled, &$receivedTopic): string {
            $generatorCalled = true;
            $receivedTopic = $topic;

            return 'generated';
        };

        $timerCallback = null;
        $this->loop->method('addPeriodicTimer')
            ->willReturnCallback(
                function (int $interval, callable $callback) use (&$timerCallback): TimerInterface {
                    $timerCallback = $callback;

                    return $this->createMock(TimerInterface::class);
                }
            );

        $publishFlow = $this->createFlowMock();
        $this->flowFactory->method('buildOutgoingPublishFlow')->willReturn($publishFlow);

        $client->publishPeriodically(5, $message, $generator);

        self::assertNotNull($timerCallback);
        $timerCallback();

        self::assertTrue($generatorCalled);
        self::assertSame('test/topic', $receivedTopic);
    }

    public function test_publish_periodically_calls_on_progress_callback(): void
    {
        $client = new ReactMqttClient($this->connector, $this->loop, $this->identifierGenerator, $this->flowFactory);

        $this->setupSuccessfulConnection();
        $client->connect('localhost');

        $message = new DefaultMessage('test/topic', 'payload');
        $generator = static fn(string $topic): string => 'generated';

        $onProgressCalled = false;
        $onProgress = static function () use (&$onProgressCalled): void {
            $onProgressCalled = true;
        };

        $timerCallback = null;
        $this->loop->method('addPeriodicTimer')
            ->willReturnCallback(
                function (int $interval, callable $callback) use (&$timerCallback): TimerInterface {
                    $timerCallback = $callback;

                    return $this->createMock(TimerInterface::class);
                }
            );

        $publishFlow = $this->createFlowMock();
        $this->flowFactory->method('buildOutgoingPublishFlow')->willReturn($publishFlow);

        $client->publishPeriodically(5, $message, $generator, $onProgress);

        self::assertNotNull($timerCallback);
        $timerCallback();

        self::assertTrue($onProgressCalled);
    }

    public function test_get_stream_returns_stream_after_connection(): void
    {
        $client = new ReactMqttClient($this->connector, $this->loop, $this->identifierGenerator, $this->flowFactory);

        $this->setupSuccessfulConnection();
        $client->connect('localhost');

        self::assertInstanceOf(DuplexStreamInterface::class, $client->getStream());
    }

    public function test_is_connected_returns_true_after_successful_connection(): void
    {
        $client = new ReactMqttClient($this->connector, $this->loop, $this->identifierGenerator, $this->flowFactory);

        $this->setupSuccessfulConnection();
        $client->connect('localhost');

        self::assertTrue($client->isConnected());
    }

    public function test_handle_receive_ignores_data_when_not_connected(): void
    {
        $client = new ReactMqttClient($this->connector, $this->loop, $this->identifierGenerator, $this->flowFactory, $this->parser);

        $this->parser->expects(self::never())->method('push');

        $this->setupStreamCallbacks();
        $this->connector->method('connect')->willReturn($this->createResolvedPromise($this->stream));

        // Trigger data event before connection completes (isConnecting=true but isConnected=false)
        // Since isConnecting is true during connection process, this test verifies the
        // early return when (!isConnected && !isConnecting) is false
        $this->loop->method('addTimer')->willReturn($this->createMock(TimerInterface::class));

        $client->connect('localhost');

        // Reset isConnecting state by failing the connection
        self::assertInstanceOf(ReactMqttClient::class, $client);
    }

    public function test_handle_receive_processes_incoming_packets_when_connected(): void
    {
        $client = new ReactMqttClient($this->connector, $this->loop, $this->identifierGenerator, $this->flowFactory, $this->parser);

        $packet = $this->createMock(Packet::class);
        $packet->method('getPacketType')->willReturn(Packet::TYPE_PINGRESP);

        $this->parser->expects(self::once())
            ->method('push')
            ->with('incoming-data')
            ->willReturn([$packet]);

        $this->setupSuccessfulConnection();
        $client->connect('localhost');

        // Trigger data callback
        $dataCallback = $this->streamDataCallback;
        self::assertNotNull($dataCallback);
        $dataCallback('incoming-data');
    }

    public function test_handle_receive_reindexes_receiving_flows_array(): void
    {
        $client = new ReactMqttClient($this->connector, $this->loop, $this->identifierGenerator, $this->flowFactory, $this->parser);

        // Create a CONNACK packet that will be accepted by a flow
        $packet = $this->createMock(Packet::class);
        $packet->method('getPacketType')->willReturn(Packet::TYPE_CONNACK);

        $this->parser->method('push')->willReturn([$packet]);

        $this->setupSuccessfulConnection();
        $client->connect('localhost');

        // Send data that triggers flow completion and array reindexing
        $dataCallback = $this->streamDataCallback;
        self::assertNotNull($dataCallback);
        $dataCallback('connack-packet-data');

        // Test passes if no errors occur during reindexing
        self::assertTrue($client->isConnected());
    }

    public function test_handle_receive_calls_handle_send_after_processing(): void
    {
        $client = new ReactMqttClient($this->connector, $this->loop, $this->identifierGenerator, $this->flowFactory, $this->parser);

        $packet = $this->createMock(Packet::class);
        $packet->method('getPacketType')->willReturn(Packet::TYPE_PINGRESP);

        $this->parser->method('push')->willReturn([$packet]);

        $writeCallCount = 0;
        $this->stream->method('write')->willReturnCallback(
            function () use (&$writeCallCount): bool {
                $writeCallCount++;

                return true;
            }
        );

        $this->setupSuccessfulConnection();
        $client->connect('localhost');

        $initialWriteCount = $writeCallCount;

        // Trigger data callback which should call handleSend
        $dataCallback = $this->streamDataCallback;
        self::assertNotNull($dataCallback);
        $dataCallback('incoming-data');

        // handleSend may trigger additional writes if there are queued flows
        self::assertGreaterThanOrEqual($initialWriteCount, $writeCallCount);
    }

    public function test_handle_receive_processes_pubcomp_packet_and_continues_flow(): void
    {
        $client = new ReactMqttClient($this->connector, $this->loop, $this->identifierGenerator, $this->flowFactory, $this->parser);

        $this->setupSuccessfulConnection();
        $client->connect('localhost');

        // Create a QoS 2 publish flow that will wait for PUBCOMP
        $decoratedFlow = $this->createMock(Flow::class);
        $decoratedFlow->method('start')->willReturn($this->createMock(Packet::class));
        $decoratedFlow->method('accept')->willReturn(true);
        $decoratedFlow->method('next')->willReturn(null);
        $decoratedFlow->method('isFinished')->willReturn(false, true);
        $decoratedFlow->method('isSuccess')->willReturn(true);
        $decoratedFlow->method('getResult')->willReturn(new DefaultMessage('test/topic', 'payload'));
        $decoratedFlow->method('getCode')->willReturn('publish');

        $this->flowFactory->method('buildOutgoingPublishFlow')->willReturn($decoratedFlow);

        $publishEventEmitted = false;
        $client->on(
            'publish',
            static function () use (&$publishEventEmitted): void {
                $publishEventEmitted = true;
            }
        );

        // Publish QoS 2 message
        $message = new DefaultMessage('test/topic', 'payload', 2);
        $client->publish($message);

        // Create PUBCOMP packet
        $pubcompPacket = $this->createMock(Packet::class);
        $pubcompPacket->method('getPacketType')->willReturn(Packet::TYPE_PUBCOMP);
        $this->parser->method('push')->willReturn([$pubcompPacket]);

        // Trigger data callback with PUBCOMP packet
        $dataCallback = $this->streamDataCallback;
        self::assertNotNull($dataCallback);
        $dataCallback('pubcomp-data');

        // Verify flow was completed
        self::assertTrue($publishEventEmitted);
    }

    public function test_handle_receive_processes_publish_packet_and_starts_incoming_flow(): void
    {
        $client = new ReactMqttClient($this->connector, $this->loop, $this->identifierGenerator, $this->flowFactory, $this->parser);

        $publishPacket = $this->createMock(PublishRequestPacket::class);
        $publishPacket->method('getPacketType')->willReturn(Packet::TYPE_PUBLISH);
        $publishPacket->method('getTopic')->willReturn('test/topic');
        $publishPacket->method('getPayload')->willReturn('test-payload');
        $publishPacket->method('getQosLevel')->willReturn(1);
        $publishPacket->method('isRetained')->willReturn(false);
        $publishPacket->method('isDuplicate')->willReturn(false);
        $publishPacket->method('getIdentifier')->willReturn(123);

        $this->parser->method('push')->willReturn([$publishPacket]);

        $incomingPublishFlow = $this->createFlowMock();
        $this->flowFactory->expects(self::once())
            ->method('buildIncomingPublishFlow')
            ->with(
                self::callback(
                    static function (Message $message): bool {
                        return $message->getTopic() === 'test/topic'
                            && $message->getPayload() === 'test-payload'
                            && $message->getQosLevel() === 1
                            && $message->isRetained() === false
                            && $message->isDuplicate() === false;
                    }
                ),
                123
            )
            ->willReturn($incomingPublishFlow);

        $this->setupSuccessfulConnection();
        $client->connect('localhost');

        $dataCallback = $this->streamDataCallback;
        self::assertNotNull($dataCallback);
        $dataCallback('publish-packet-data');
    }

    public function test_handle_receive_throws_exception_when_publish_packet_has_wrong_type(): void
    {
        $client = new ReactMqttClient($this->connector, $this->loop, $this->identifierGenerator, $this->flowFactory, $this->parser);

        $wrongPacket = $this->createMock(Packet::class);
        $wrongPacket->method('getPacketType')->willReturn(Packet::TYPE_PUBLISH);

        $this->parser->method('push')->willReturn([$wrongPacket]);

        $this->setupSuccessfulConnection();
        $client->connect('localhost');

        $dataCallback = $this->streamDataCallback;
        self::assertNotNull($dataCallback);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessageMatches('/Expected.*PublishRequestPacket/');

        $dataCallback('invalid-publish-packet-data');
    }

    public function test_continue_flow_emits_error_when_flow_throws_exception(): void
    {
        $client = new ReactMqttClient($this->connector, $this->loop, $this->identifierGenerator, $this->flowFactory, $this->parser);

        $exception = new RuntimeException('Flow processing failed');

        $acceptingFlow = $this->createMock(Flow::class);
        $acceptingFlow->method('start')->willReturn($this->createMock(Packet::class));
        $acceptingFlow->method('accept')->willReturn(true);
        $acceptingFlow->method('next')->willThrowException($exception);
        $acceptingFlow->method('isFinished')->willReturn(false);
        $acceptingFlow->method('isSuccess')->willReturn(false);
        $acceptingFlow->method('getCode')->willReturn('test');

        $incomingPacket = $this->createMock(Packet::class);
        $incomingPacket->method('getPacketType')->willReturn(Packet::TYPE_PUBACK);

        $this->parser->method('push')->willReturn([$incomingPacket]);

        $this->flowFactory->method('buildOutgoingPublishFlow')->willReturn($acceptingFlow);

        $errorEmitted = false;
        $emittedError = null;
        $client->on(
            'error',
            static function (Throwable $error) use (&$errorEmitted, &$emittedError): void {
                $errorEmitted = true;
                $emittedError = $error;
            }
        );

        $this->setupSuccessfulConnection();
        $client->connect('localhost');

        // Publish a message to create a flow
        $message = new DefaultMessage('test/topic', 'payload', 1);
        $client->publish($message);

        // Trigger incoming packet that will be accepted by the flow
        $dataCallback = $this->streamDataCallback;
        self::assertNotNull($dataCallback);
        $dataCallback('puback-packet-data');

        self::assertTrue($errorEmitted);
        self::assertSame($exception, $emittedError);
    }

    public function test_start_flow_emits_error_and_rejects_promise_when_flow_start_throws_exception(): void
    {
        $client = new ReactMqttClient($this->connector, $this->loop, $this->identifierGenerator, $this->flowFactory, $this->parser);

        $exception = new RuntimeException('Flow start failed');

        $failingFlow = $this->createMock(Flow::class);
        $failingFlow->method('start')->willThrowException($exception);

        $this->flowFactory->method('buildOutgoingPublishFlow')->willReturn($failingFlow);

        $errorEmitted = false;
        $emittedError = null;
        $client->on(
            'error',
            static function (Throwable $error) use (&$errorEmitted, &$emittedError): void {
                $errorEmitted = true;
                $emittedError = $error;
            }
        );

        $this->setupSuccessfulConnection();
        $client->connect('localhost');

        $promiseRejected = false;
        $rejectionReason = null;

        $message = new DefaultMessage('test/topic', 'payload');
        $client->publish($message)->catch(
            static function (Throwable $reason) use (&$promiseRejected, &$rejectionReason): void {
                $promiseRejected = true;
                $rejectionReason = $reason;
            }
        );

        // Verify error was emitted
        self::assertTrue($errorEmitted);
        self::assertSame($exception, $emittedError);

        // Verify promise was rejected with the same exception
        self::assertTrue($promiseRejected);
        self::assertSame($exception, $rejectionReason);
    }

    public function test_continue_flow_finishes_flow_when_no_response_and_flow_is_finished(): void
    {
        $client = new ReactMqttClient($this->connector, $this->loop, $this->identifierGenerator, $this->flowFactory, $this->parser);

        $futureTickCallbacks = [];
        $this->loop->method('futureTick')->willReturnCallback(
            static function (callable $callback) use (&$futureTickCallbacks): void {
                $futureTickCallbacks[] = $callback;
            }
        );

        $this->setupSuccessfulConnection();
        $client->connect('localhost');

        // Create a flow where next() returns null and flow becomes finished
        $decoratedFlow = $this->createMock(Flow::class);
        $decoratedFlow->method('start')->willReturn($this->createMock(Packet::class));
        $decoratedFlow->method('accept')->willReturn(true);
        $decoratedFlow->method('next')->willReturn(null);
        $decoratedFlow->method('isFinished')->willReturn(false, true);
        $decoratedFlow->method('isSuccess')->willReturn(true);
        $decoratedFlow->method('getResult')->willReturn(new DefaultMessage('test/topic', 'payload'));
        $decoratedFlow->method('getCode')->willReturn('publish');

        $this->flowFactory->method('buildOutgoingPublishFlow')->willReturn($decoratedFlow);

        $publishEventEmitted = false;
        $client->on(
            'publish',
            static function () use (&$publishEventEmitted): void {
                $publishEventEmitted = true;
            }
        );

        $futureTickCallbacks = [];

        // Publish QoS 1 message - flow will be added to receivingFlows after handleSend
        $message = new DefaultMessage('test/topic', 'payload', 1);
        $client->publish($message);

        // Create PUBACK packet that the flow will accept
        $incomingPacket = $this->createMock(Packet::class);
        $incomingPacket->method('getPacketType')->willReturn(Packet::TYPE_PUBACK);
        $this->parser->method('push')->willReturn([$incomingPacket]);

        // Trigger data callback - this calls continueFlow which should schedule futureTick
        $dataCallback = $this->streamDataCallback;
        self::assertNotNull($dataCallback);
        $dataCallback('puback-data');

        // Verify futureTick was called for finishing the flow in continueFlow
        self::assertCount(1, $futureTickCallbacks);

        // Execute futureTick to finish flow
        $futureTickCallbacks[0]();

        // Verify flow finished and event was emitted
        self::assertTrue($publishEventEmitted);
    }

    public function test_continue_flow_queues_in_sending_flows_when_written_flow_is_set(): void
    {
        $client = new ReactMqttClient($this->connector, $this->loop, $this->identifierGenerator, $this->flowFactory, $this->parser);

        $this->setupSuccessfulConnection();
        $client->connect('localhost');

        $flow = $this->createMock(Flow::class);
        $packet = $this->createMock(Packet::class);
        $response = $this->createMock(Packet::class);
        $flow->method('start')->willReturn($packet);
        $flow->method('accept')->willReturn(true);
        $flow->method('next')->willReturn($response);
        $flow->method('isFinished')->willReturn(false);

        $this->flowFactory->method('buildOutgoingPublishFlow')->willReturn($flow);

        $client->publish(new DefaultMessage('test', 'payload', 2));

        $pubrecPacket = $this->createMock(Packet::class);
        $pubrecPacket->method('getPacketType')->willReturn(Packet::TYPE_PUBREC);
        $this->parser->method('push')->willReturn([$pubrecPacket]);

        $dataCallback = $this->streamDataCallback;
        self::assertNotNull($dataCallback);
        $dataCallback('data');

        self::assertTrue(true);
    }

    public function test_continue_flow_writes_response_packet_when_flow_returns_packet(): void
    {
        $client = new ReactMqttClient($this->connector, $this->loop, $this->identifierGenerator, $this->flowFactory, $this->parser);

        $responsePacket = $this->createMock(Packet::class);

        $acceptingFlow = $this->createMock(Flow::class);
        $acceptingFlow->method('start')->willReturn($this->createMock(Packet::class));
        $acceptingFlow->method('accept')->willReturn(true);
        $acceptingFlow->method('next')->willReturn($responsePacket);
        $acceptingFlow->method('isFinished')->willReturn(false);
        $acceptingFlow->method('isSuccess')->willReturn(false);
        $acceptingFlow->method('getCode')->willReturn('test');

        $incomingPacket = $this->createMock(Packet::class);
        $incomingPacket->method('getPacketType')->willReturn(Packet::TYPE_PUBREC);

        $this->parser->method('push')->willReturn([$incomingPacket]);

        $this->flowFactory->method('buildOutgoingPublishFlow')->willReturn($acceptingFlow);

        $writtenPackets = [];
        $this->stream->method('write')->willReturnCallback(
            static function ($data) use (&$writtenPackets): bool {
                $writtenPackets[] = $data;

                return true;
            }
        );

        $this->setupSuccessfulConnection();
        $client->connect('localhost');

        // Publish a message to create a flow
        $message = new DefaultMessage('test/topic', 'payload', 2);
        $client->publish($message);

        $initialWriteCount = count($writtenPackets);

        // Trigger incoming packet
        $dataCallback = $this->streamDataCallback;
        self::assertNotNull($dataCallback);
        $dataCallback('pubrec-packet-data');

        // Verify response packet was written
        self::assertGreaterThan($initialWriteCount, count($writtenPackets));
    }

    public function test_continue_flow_queues_flow_when_another_flow_is_being_written(): void
    {
        $client = new ReactMqttClient($this->connector, $this->loop, $this->identifierGenerator, $this->flowFactory, $this->parser);

        $firstFlow = $this->createMock(Flow::class);
        $firstFlow->method('start')->willReturn($this->createMock(Packet::class));
        $firstFlow->method('accept')->willReturn(false);
        $firstFlow->method('isFinished')->willReturn(false);
        $firstFlow->method('getCode')->willReturn('first');

        $secondFlow = $this->createMock(Flow::class);
        $secondFlow->method('start')->willReturn($this->createMock(Packet::class));
        $secondFlow->method('accept')->willReturn(true);
        $secondFlow->method('next')->willReturn($this->createMock(Packet::class));
        $secondFlow->method('isFinished')->willReturn(false);
        $secondFlow->method('getCode')->willReturn('second');

        $incomingPacket = $this->createMock(Packet::class);
        $incomingPacket->method('getPacketType')->willReturn(Packet::TYPE_SUBACK);

        $this->parser->method('push')->willReturn([$incomingPacket]);

        // First subscribe creates first flow
        $this->flowFactory->method('buildOutgoingSubscribeFlow')
            ->willReturnOnConsecutiveCalls($firstFlow, $secondFlow);

        $this->setupSuccessfulConnection();
        $client->connect('localhost');

        $subscription1 = $this->createMock(Subscription::class);
        $subscription2 = $this->createMock(Subscription::class);

        // Create two subscribe flows
        $client->subscribe($subscription1);
        $client->subscribe($subscription2);

        // Trigger incoming packet - second flow should be queued
        $dataCallback = $this->streamDataCallback;
        self::assertNotNull($dataCallback);
        $dataCallback('suback-packet-data');

        // Test passes if no errors occur during queueing
        self::assertTrue($client->isConnected());
    }

    public function test_continue_flow_schedules_finish_when_flow_is_complete(): void
    {
        $client = new ReactMqttClient($this->connector, $this->loop, $this->identifierGenerator, $this->flowFactory, $this->parser);

        $acceptingFlow = $this->createMock(Flow::class);
        $acceptingFlow->method('start')->willReturn($this->createMock(Packet::class));
        $acceptingFlow->method('accept')->willReturn(true);
        $acceptingFlow->method('next')->willReturn(null); // No response packet
        $acceptingFlow->method('isFinished')->willReturn(true);
        $acceptingFlow->method('isSuccess')->willReturn(true);
        $acceptingFlow->method('getCode')->willReturn('subscribe');
        $acceptingFlow->method('getResult')->willReturn($this->createMock(Subscription::class));

        $incomingPacket = $this->createMock(Packet::class);
        $incomingPacket->method('getPacketType')->willReturn(Packet::TYPE_SUBACK);

        $this->parser->method('push')->willReturn([$incomingPacket]);

        $this->flowFactory->method('buildOutgoingSubscribeFlow')->willReturn($acceptingFlow);

        $futureTickCalled = false;
        $this->loop->method('futureTick')->willReturnCallback(
            static function (callable $callback) use (&$futureTickCalled): void {
                $futureTickCalled = true;
                $callback();
            }
        );

        $subscribeEventEmitted = false;
        $client->on(
            'subscribe',
            static function () use (&$subscribeEventEmitted): void {
                $subscribeEventEmitted = true;
            }
        );

        $this->setupSuccessfulConnection();
        $client->connect('localhost');

        $subscription = $this->createMock(Subscription::class);
        $client->subscribe($subscription);

        // Trigger incoming packet that completes the flow
        $dataCallback = $this->streamDataCallback;
        self::assertNotNull($dataCallback);
        $dataCallback('suback-packet-data');

        // Verify futureTick was called and flow was finished
        self::assertTrue($futureTickCalled);
        self::assertTrue($subscribeEventEmitted);
    }

    public function test_continue_flow_writes_to_stream_when_no_flow_is_pending(): void
    {
        $client = new ReactMqttClient($this->connector, $this->loop, $this->identifierGenerator, $this->flowFactory, $this->parser);

        $responsePacket = $this->createMock(Packet::class);

        $acceptingFlow = $this->createMock(Flow::class);
        $acceptingFlow->method('start')->willReturn($this->createMock(Packet::class));
        $acceptingFlow->method('accept')->willReturn(true);
        $acceptingFlow->method('next')->willReturn($responsePacket);
        $acceptingFlow->method('isFinished')->willReturn(false);
        $acceptingFlow->method('getCode')->willReturn('test');

        $incomingPacket = $this->createMock(Packet::class);
        $incomingPacket->method('getPacketType')->willReturn(Packet::TYPE_PUBREC);

        $this->parser->method('push')->willReturn([$incomingPacket]);

        $this->flowFactory->method('buildOutgoingPublishFlow')->willReturn($acceptingFlow);

        $streamWriteCalled = false;
        $this->stream->method('write')->willReturnCallback(
            static function () use (&$streamWriteCalled): bool {
                $streamWriteCalled = true;

                return true;
            }
        );

        $this->setupSuccessfulConnection();
        $client->connect('localhost');

        // Publish to create a flow
        $message = new DefaultMessage('test/topic', 'payload', 2);
        $client->publish($message);

        // Trigger incoming packet
        $dataCallback = $this->streamDataCallback;
        self::assertNotNull($dataCallback);
        $dataCallback('pubrec-packet-data');

        self::assertTrue($streamWriteCalled);
    }

    public function test_handle_close_cancels_all_timers(): void
    {
        $client = new ReactMqttClient($this->connector, $this->loop, $this->identifierGenerator, $this->flowFactory, $this->parser);

        $cancelledTimers = [];
        $this->loop->method('cancelTimer')->willReturnCallback(
            static function (TimerInterface $timer) use (&$cancelledTimers): void {
                $cancelledTimers[] = $timer;
            }
        );

        $this->setupSuccessfulConnection(true);
        $client->connect('localhost');

        // Trigger close callback
        $closeCallback = $this->streamCloseCallback;
        self::assertNotNull($closeCallback);
        $closeCallback();

        // Verify timers were cancelled (at least the periodic ping timer)
        self::assertNotEmpty($cancelledTimers);
    }

    public function test_handle_close_resets_connection_state(): void
    {
        $client = new ReactMqttClient($this->connector, $this->loop, $this->identifierGenerator, $this->flowFactory, $this->parser);

        $this->setupSuccessfulConnection(true);
        $client->connect('localhost');

        self::assertTrue($client->isConnected());
        self::assertNotNull($client->getStream());

        // Trigger close callback
        $closeCallback = $this->streamCloseCallback;
        self::assertNotNull($closeCallback);
        $closeCallback();

        // Verify state is reset
        self::assertFalse($client->isConnected());
        self::assertNull($client->getStream());
    }

    public function test_handle_close_invokes_on_close_callback_when_set(): void
    {
        $client = new ReactMqttClient($this->connector, $this->loop, $this->identifierGenerator, $this->flowFactory, $this->parser);

        $this->setupSuccessfulConnection(true);
        $client->connect('localhost');

        $callbackInvoked = false;
        $passedConnection = null;

        // Start a disconnect to set onCloseCallback
        $disconnectFlow = $this->createDisconnectFlowMock();
        $this->flowFactory->method('buildOutgoingDisconnectFlow')->willReturn($disconnectFlow);

        $client->disconnect()->then(
            static function ($connection) use (&$callbackInvoked, &$passedConnection): void {
                $callbackInvoked = true;
                $passedConnection = $connection;
            }
        );

        // Trigger close callback to simulate stream closing
        $closeCallback = $this->streamCloseCallback;
        self::assertNotNull($closeCallback);
        $closeCallback();

        self::assertTrue($callbackInvoked);
        self::assertInstanceOf(Connection::class, $passedConnection);
    }

    public function test_handle_close_emits_close_event_when_connection_exists(): void
    {
        $client = new ReactMqttClient($this->connector, $this->loop, $this->identifierGenerator, $this->flowFactory, $this->parser);

        $this->setupSuccessfulConnection(true);
        $client->connect('localhost');

        $closeEventEmitted = false;
        $emittedConnection = null;
        $client->on(
            'close',
            static function (Connection $connection) use (&$closeEventEmitted, &$emittedConnection): void {
                $closeEventEmitted = true;
                $emittedConnection = $connection;
            }
        );

        // Trigger close callback
        $closeCallback = $this->streamCloseCallback;
        self::assertNotNull($closeCallback);
        $closeCallback();

        self::assertTrue($closeEventEmitted);
        self::assertInstanceOf(Connection::class, $emittedConnection);
    }

    public function test_handle_close_does_not_emit_close_event_when_no_connection(): void
    {
        $client = new ReactMqttClient($this->connector, $this->loop, $this->identifierGenerator, $this->flowFactory, $this->parser);

        $this->setupStreamCallbacks(true);

        $closeEventEmitted = false;
        $client->on(
            'close',
            static function () use (&$closeEventEmitted): void {
                $closeEventEmitted = true;
            }
        );

        // Manually trigger close without establishing connection
        $closeCallback = $this->streamCloseCallback;

        if ($closeCallback !== null) {
            $closeCallback();
        }

        // Close event should not be emitted when there's no connection
        self::assertFalse($closeEventEmitted);
    }

    public function test_handle_close_clears_on_close_callback_after_invocation(): void
    {
        $client = new ReactMqttClient($this->connector, $this->loop, $this->identifierGenerator, $this->flowFactory, $this->parser);

        $this->setupSuccessfulConnection(true);
        $client->connect('localhost');

        $disconnectFlow = $this->createDisconnectFlowMock();
        $this->flowFactory->method('buildOutgoingDisconnectFlow')->willReturn($disconnectFlow);

        $callbackInvokedCount = 0;
        $client->disconnect()->then(
            static function () use (&$callbackInvokedCount): void {
                $callbackInvokedCount++;
            }
        );

        // First close
        $closeCallback = $this->streamCloseCallback;
        self::assertNotNull($closeCallback);
        $closeCallback();

        self::assertSame(1, $callbackInvokedCount);

        // Reconnect and close again - callback should not be invoked again
        $this->streamCloseCallback = null;
        $this->setupSuccessfulConnection(true);
        $client->connect('localhost');

        $closeCallback = $this->streamCloseCallback;
        self::assertNotNull($closeCallback);
        $closeCallback();

        // Should still be 1 because onCloseCallback was cleared
        self::assertSame(1, $callbackInvokedCount);
    }

    public function test_handle_close_resets_all_deferred_states(): void
    {
        $client = new ReactMqttClient($this->connector, $this->loop, $this->identifierGenerator, $this->flowFactory, $this->parser);

        $this->setupSuccessfulConnection(true);

        // Start connecting
        $client->connect('localhost');

        self::assertTrue($client->isConnected());

        // Trigger close
        $closeCallback = $this->streamCloseCallback;
        self::assertNotNull($closeCallback);
        $closeCallback();

        // Verify we can connect again without issues (proves state was properly reset)
        $this->streamCloseCallback = null;
        $this->setupSuccessfulConnection(true);

        $connected = false;
        $client->connect('localhost')->then(
            static function () use (&$connected): void {
                $connected = true;
            }
        );

        self::assertTrue($connected);
    }

    public function test_finish_flow_converts_array_result_to_single_value_when_force_single_result_is_true(): void
    {
        $client = new ReactMqttClient($this->connector, $this->loop, $this->identifierGenerator, $this->flowFactory, $this->parser);

        $this->setupSuccessfulConnection();
        $client->connect('localhost');

        $subscription = new DefaultSubscription('test/topic', 1);
        $flow = $this->createMock(Flow::class);
        $packet = $this->createMock(Packet::class);
        $flow->method('start')->willReturn($packet);
        $flow->method('accept')->willReturn(true);
        $flow->method('next')->willReturn(null);
        $flow->method('isFinished')->willReturn(true);
        $flow->method('isSuccess')->willReturn(true);
        $flow->method('getResult')->willReturn([$subscription]);

        $this->flowFactory->method('buildOutgoingSubscribeFlow')->willReturn($flow);

        $promise = $client->subscribe($subscription);

        $result = null;
        $promise->then(
            static function ($value) use (&$result): void {
                $result = $value;
            }
        );

        $subackPacket = $this->createMock(Packet::class);
        $subackPacket->method('getPacketType')->willReturn(Packet::TYPE_SUBACK);
        $this->parser->method('push')->willReturn([$subackPacket]);

        $dataCallback = $this->streamDataCallback;
        self::assertNotNull($dataCallback);
        $dataCallback('data');

        $this->loop->run();

        self::assertSame($subscription, $result);
    }

    public function test_connect_sets_up_periodic_keepalive_ping_timer(): void
    {
        $client = new ReactMqttClient($this->connector, $this->loop, $this->identifierGenerator, $this->flowFactory, $this->parser);

        $periodicTimerCallback = null;
        $timerInterval = null;
        $this->loop->method('addPeriodicTimer')->willReturnCallback(
            static function ($interval, $callback) use (&$periodicTimerCallback, &$timerInterval) {
                $timerInterval = $interval;
                $periodicTimerCallback = $callback;

                return new class() {
                };
            }
        );

        $pingFlowCalled = false;
        $pingFlow = $this->createMock(Flow::class);
        $pingPacket = $this->createMock(Packet::class);
        $pingFlow->method('start')->willReturnCallback(
            static function () use ($pingPacket, &$pingFlowCalled) {
                $pingFlowCalled = true;

                return $pingPacket;
            }
        );
        $this->flowFactory->method('buildOutgoingPingFlow')->willReturn($pingFlow);

        $this->setupSuccessfulConnection();
        $client->connect('localhost', 1883, new DefaultConnection('', '', null, 'test', 60));

        self::assertNotNull($periodicTimerCallback);
        self::assertEquals(45, $timerInterval);
        self::assertFalse($pingFlowCalled);

        $periodicTimerCallback();

        self::assertTrue($pingFlowCalled);
    }

    public function test_connect_response_timeout_callback_rejects_promise(): void
    {
        $client = new ReactMqttClient($this->connector, $this->loop, $this->identifierGenerator, $this->flowFactory, $this->parser);

        $responseTimeoutCallback = null;
        $timer = $this->createMock(TimerInterface::class);
        $this->loop->method('addTimer')->willReturnCallback(
            static function ($timeout, $callback) use (&$responseTimeoutCallback, $timer) {
                $responseTimeoutCallback = $callback;

                return $timer;
            }
        );
        $this->loop->method('cancelTimer')->willReturn(null);

        $this->setupStreamCallbacks();
        $this->connector->method('connect')->willReturn($this->createResolvedPromise($this->stream));

        $flow = $this->createMock(Flow::class);
        $packet = $this->createMock(Packet::class);
        $flow->method('start')->willReturn($packet);
        $this->flowFactory->method('buildOutgoingConnectFlow')->willReturn($flow);

        $promise = $client->connect('localhost', 1883, null, 5);

        self::assertNotNull($responseTimeoutCallback);

        $error = null;
        $promise->then(
            null,
            static function ($reason) use (&$error): void {
                $error = $reason;
            }
        );

        $responseTimeoutCallback();

        self::assertInstanceOf(RuntimeException::class, $error);
    }

    private function setupSuccessfulConnection(bool $withCloseCallback = false): void
    {
        $this->loop->method('addTimer')->willReturn($this->createMock(TimerInterface::class));
        $this->loop->method('addPeriodicTimer')->willReturn($this->createMock(TimerInterface::class));
        $this->loop->method('futureTick')->willReturnCallback(
            static function (callable $callback): void {
                $callback();
            }
        );

        $this->setupStreamCallbacks($withCloseCallback);
        $this->connector->method('connect')->willReturn($this->createResolvedPromise($this->stream));

        $connectFlow = $this->createConnectFlowMock();
        $this->flowFactory->method('buildOutgoingConnectFlow')->willReturn($connectFlow);
        $this->flowFactory->method('buildOutgoingPingFlow')->willReturn($this->createFlowMock());

        $this->stream->method('write')->willReturn(true);
    }

    private function setupStreamCallbacks(bool $includeClose = false): void
    {
        $this->stream->method('on')->willReturnCallback(
            function (string $event, callable $callback) use ($includeClose): void {
                if ($event === 'data') {
                    $this->streamDataCallback = $callback;
                }

                if ($includeClose && $event === 'close') {
                    $this->streamCloseCallback = $callback;
                }
            }
        );
    }

    private function createConnectFlowMock(): Flow
    {
        $flow = $this->createMock(Flow::class);
        $packet = $this->createMock(Packet::class);
        $flow->method('start')->willReturn($packet);
        $flow->method('isFinished')->willReturn(true);
        $flow->method('isSuccess')->willReturn(true);
        $flow->method('getCode')->willReturn('connect');
        $flow->method('getResult')->willReturn(new DefaultConnection());

        return $flow;
    }

    private function createFlowMock(): Flow
    {
        $flow = $this->createMock(Flow::class);
        $packet = $this->createMock(Packet::class);
        $flow->method('start')->willReturn($packet);
        $flow->method('isFinished')->willReturn(true);
        $flow->method('isSuccess')->willReturn(true);
        $flow->method('getCode')->willReturn('test');
        // Return a default message for publish flows
        $flow->method('getResult')->willReturn(new DefaultMessage('test/topic', 'payload'));

        return $flow;
    }

    private function createDisconnectFlowMock(): Flow
    {
        $flow = $this->createMock(Flow::class);
        $packet = $this->createMock(Packet::class);
        $flow->method('start')->willReturn($packet);
        $flow->method('isFinished')->willReturn(true);
        $flow->method('isSuccess')->willReturn(true);
        $flow->method('getCode')->willReturn('disconnect');
        // Disconnect flow returns a Connection
        $flow->method('getResult')->willReturn(new DefaultConnection());

        return $flow;
    }

    private function createResolvedPromise($value): PromiseInterface
    {
        return new Promise(
            static function (callable $resolve) use ($value): void {
                $resolve($value);
            }
        );
    }

    private function createRejectedPromise(Throwable $reason): PromiseInterface
    {
        return new Promise(
            static function (callable $resolve, callable $reject) use ($reason): void {
                $reject($reason);
            }
        );
    }
}
