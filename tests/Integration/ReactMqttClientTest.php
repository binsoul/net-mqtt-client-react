<?php

declare(strict_types=1);

namespace BinSoul\Test\Net\Mqtt\Client\React\Integration;

use BinSoul\Net\Mqtt\Client\React\ReactMqttClient;
use BinSoul\Net\Mqtt\Connection;
use BinSoul\Net\Mqtt\DefaultConnection;
use BinSoul\Net\Mqtt\DefaultMessage;
use BinSoul\Net\Mqtt\DefaultSubscription;
use BinSoul\Net\Mqtt\Message;
use BinSoul\Net\Mqtt\Subscription;
use Exception;
use PHPUnit\Framework\TestCase;
use React\EventLoop\Loop;
use React\EventLoop\LoopInterface;
use React\Socket\Connector;

/**
 * Tests the ReactMqttClient class.
 *
 * @group   integration
 *
 * @author  Alin Eugen Deac <ade@vestergaardcompany.com>
 */
class ReactMqttClientTest extends TestCase
{
    /**
     * The default hostname.
     *
     * @see http://iot.eclipse.org/getting-started
     *
     * @var string
     */
    private const DEFAULT_HOSTNAME = 'broker.hivemq.com';

    /**
     * The default port.
     *
     * 1883, unsecured connection
     * 8883, secure connection
     *
     * @var int
     */
    private const DEFAULT_PORT = 1883;

    /**
     * The default timeout for the connection attempt.
     *
     * @var int
     */
    private const DEFAULT_CONNECT_TIMEOUT = 30;

    /**
     * The topic prefix.
     *
     * @var string
     */
    private const TOPIC_PREFIX = 'testing/BinSoul/';

    /**
     * Loop timeout, duration in seconds.
     *
     * @var int
     */
    private const MAXIMUM_EXECUTION_TIME = 60;

    /**
     * The address or domain name of the server.
     */
    private string $hostname;

    /**
     * The port number used for the connection.
     */
    private int $port;

    /**
     * The duration in seconds to wait before connection times out.
     */
    private int $connectTimeout;

    private LoopInterface $loop;

    private ReactMqttClient $client;

    /**
     * @var array<array{mixed, mixed, string}>
     */
    private array $expectations = [];

    /**
     * @var array<int, string>
     */
    private array $failures = [];

    protected function setUp(): void
    {
        $this->hostname = $_ENV['MQTT_HOSTNAME'] ?? $_SERVER['MQTT_HOSTNAME'] ?? self::DEFAULT_HOSTNAME;
        $this->port = $_ENV['MQTT_PORT'] ?? $_SERVER['MQTT_PORT'] ?? self::DEFAULT_PORT;
        $this->connectTimeout = $_ENV['MQTT_CONNECT_TIMEOUT'] ?? $_SERVER['MQTT_CONNECT_TIMEOUT'] ?? self::DEFAULT_CONNECT_TIMEOUT;

        // Create event loop
        $this->loop = Loop::get();

        // Add loop timeout
        $this->loop->addPeriodicTimer(
            self::MAXIMUM_EXECUTION_TIME,
            function (): void {
                $this->loop->stop();
                $this->fail('Test timeout');
            }
        );

        echo 'Test: ' . str_replace(['test_', '_'], ['', ' '], $this->getName()) . PHP_EOL . PHP_EOL;
    }

    protected function tearDown(): void
    {
        $this->loop->stop();

        echo str_repeat('- - ', 25) . PHP_EOL;
    }

    /*******************************************************
     * Tests
     ******************************************************/

    /**
     * Tests that a client can successfully connect to a broker.
     */
    public function test_connect_success(): void
    {
        $client = $this->buildClient();
        $client->connect($this->hostname, $this->port, null, $this->connectTimeout)
            ->then(
                function (?Connection $connection) use ($client): void {
                    $this->addExpectation(true, $client->isConnected(), 'Client should be connected');
                    $this->addExpectation(true, is_object($connection), 'Connection should be an object');
                    $this->stopLoop();
                }
            )
            ->catch(
                function () use ($client): void {
                    $this->addExpectation(false, $client->isConnected(), 'Client should not be connected');
                    $this->addFailure('Failed to connect.');
                    $this->stopLoop();
                }
            );

        $this->startLoop();
        $this->handleExpectations();
    }

    /**
     * Tests that a client fails to connect to a invalid broker.
     *
     * @depends test_connect_success
     */
    public function test_connect_failure(): void
    {
        $client = $this->buildClient();
        $client->connect($this->hostname, 12345, null, 1)
            ->then(
                function (?Connection $connection) use ($client): void {
                    $this->addExpectation(false, $client->isConnected(), 'Client should not be connected');
                    $this->addExpectation(true, $connection === null, 'Connection should not be null');
                    $this->stopLoop();
                }
            )
            ->catch(
                function () use ($client): void {
                    $this->addExpectation(false, $client->isConnected(), 'Client should not be connected');
                    $this->stopLoop();
                }
            );

        $this->startLoop();
        $this->handleExpectations();
    }

    /**
     * Test that attempting to connect when the client is already connected does work.
     */
    public function test_connect_when_already_connected(): void
    {
        $client = $this->buildClient();
        $client->connect($this->hostname, $this->port, null, $this->connectTimeout)
            ->then(
                function () use ($client): void {
                    $client->connect($this->hostname, $this->port, null, $this->connectTimeout)
                        ->then(
                            function (?Connection $connection) use ($client): void {
                                $this->addExpectation(true, $client->isConnected(), 'Client should be connected');
                                $this->addExpectation(true, is_object($connection), 'Connection should be an object');
                                $this->stopLoop();
                            }
                        )
                        ->catch(
                            function (): void {
                                $this->addFailure('Failed to connect.');
                                $this->stopLoop();
                            }
                        );
                }
            )
            ->catch(
                function (): void {
                    $this->addFailure('Failed to connect.');
                    $this->stopLoop();
                }
            );

        $this->startLoop();
        $this->handleExpectations();
    }

    /**
     * Test that connecting the client twice returns the same promise.
     */
    public function test_connect_twice(): void
    {
        $client = $this->buildClient();
        $promiseA = $client->connect($this->hostname, 12345, null, 1);
        $promiseB = $client->connect($this->hostname, 12345, null, 1);

        $this->addExpectation(true, $promiseA === $promiseB, 'Promises should be identical');

        $noop = function (): void {
        };

        $promiseA->catch($noop);
        $promiseB->catch($noop);

        $this->startLoop();
        $this->stopLoop();
        $this->handleExpectations();
    }

    /**
     * Test that client's connection state is updated correctly when connected.
     *
     * @depends test_connect_success
     */
    public function test_is_connected_when_connect_event_emitted(): void
    {
        $client = $this->buildClient();

        $client->on(
            'connect',
            function (Connection $connection) use ($client): void {
                $this->addExpectation(true, $client->isConnected(), 'Client should be connected');
                $this->addExpectation(true, is_object($connection), 'Connection should be an object');
                $this->stopLoop();
            }
        );

        $client->connect($this->hostname, $this->port, null, $this->connectTimeout)
            ->then(
                function () use ($client): void {
                    $this->addExpectation(true, $client->isConnected(), 'Client should be connected');
                    $this->stopLoop();
                }
            )
            ->catch(
                function (): void {
                    $this->addFailure('Failed to connect.');
                    $this->stopLoop();
                }
            );

        $this->startLoop();
        $this->handleExpectations();
    }

    /**
     * Test that client's disconnect method correctly handles the case where the client is not connected.
     */
    public function test_disconnect_when_not_connected(): void
    {
        $client = $this->buildClient();
        $client->disconnect()
            ->then(
                function (?Connection $connection) use ($client): void {
                    $this->addExpectation(false, $client->isConnected(), 'Client should not be connected');
                    $this->addExpectation(true, $connection === null, 'Connection should not be null');
                }
            );

        $this->handleExpectations();
    }

    /**
     * Test that disconnecting a client twice returns the same promise.
     */
    public function test_disconnect_twice(): void
    {
        $client = $this->buildClient();
        $client->connect($this->hostname, $this->port, null, $this->connectTimeout)
            ->then(
                function () use ($client): void {
                    $promiseA = $client->disconnect();
                    $promiseB = $client->disconnect();

                    $this->addExpectation(true, $promiseA === $promiseB, 'Promises should be identical');

                    $this->stopLoop();
                }
            )
            ->catch(
                function (): void {
                    $this->addFailure('Failed to connect.');
                    $this->stopLoop();
                }
            );

        $this->startLoop();
        $this->handleExpectations();
    }

    /**
     * Test that client's connection state is updated correctly when disconnected.
     *
     * @depends test_connect_success
     */
    public function test_is_disconnected_when_disconnect_event_emitted(): void
    {
        $client = $this->buildClient();

        $client->on(
            'disconnect',
            function (Connection $connection) use ($client): void {
                $this->addExpectation(false, $client->isConnected(), 'Client should not be connected');
                $this->addExpectation(true, is_object($connection), 'Connection should be an object');
                $this->stopLoop();
            }
        );

        $client->connect($this->hostname, $this->port, null, $this->connectTimeout)
            ->then(
                function () use ($client): void {
                    $client->disconnect()
                        ->then(
                            function (?Connection $connection) use ($client): void {
                                $this->addExpectation(false, $client->isConnected(), 'Client should not be connected');
                                $this->addExpectation(true, is_object($connection), 'Connection should be an object');
                                $this->stopLoop();
                            }
                        );
                }
            )
            ->catch(
                function (): void {
                    $this->addFailure('Failed to connect.');
                    $this->stopLoop();
                }
            );

        $this->startLoop();
        $this->handleExpectations();
    }

    /**
     * Tests that messages can be send and received successfully.
     *
     * @depends test_connect_success
     */
    public function test_send_and_receive_message_qos_level_0(): void
    {
        $subscription = $this->generateSubscription(0);
        $message = new DefaultMessage($subscription->getFilter(), 'qos=0', 0);

        $this->subscribeAndPublish($subscription, $message);
    }

    /**
     * Tests that messages can be send and received successfully with QoS level 1.
     *
     * @depends test_connect_success
     */
    public function test_send_and_receive_message_qos_level_1(): void
    {
        $subscription = $this->generateSubscription(1);
        $message = new DefaultMessage($subscription->getFilter(), 'qos=1', 1);

        $this->subscribeAndPublish($subscription, $message);
    }

    /**
     * Tests that messages can be send and received successfully with QoS level 2.
     *
     * @depends test_connect_success
     */
    public function test_send_and_receive_message_qos_level_2(): void
    {
        $subscription = $this->generateSubscription(2);
        $message = new DefaultMessage($subscription->getFilter(), 'qos=2', 2);

        $this->subscribeAndPublish($subscription, $message);
    }

    /**
     * Test that client can to subscribe to a single level wildcard filter, e.g. /A/B/+/C.
     *
     * @depends test_connect_success
     */
    public function test_can_subscribe_to_single_level_wildcard_filter(): void
    {
        $subscription = $this->generateSubscription();
        $message = new DefaultMessage($subscription->getFilter() . '/A/B/foo/C', 'Never vandalize a ship.');
        $subscription = $subscription->withFilter($subscription->getFilter() . '/A/B/+/C');

        $this->subscribeAndPublish($subscription, $message);
    }

    /**
     * Test that client can to subscribe to a multi level wildcard filter, e.g. /A/B/#.
     *
     * @depends test_connect_success
     */
    public function test_can_subscribe_to_multi_level_wildcard_filter(): void
    {
        $subscription = $this->generateSubscription();
        $message = new DefaultMessage($subscription->getFilter() . '/A/B/foo/bar/baz/C', 'Never sail a kraken.');
        $subscription = $subscription->withFilter($subscription->getFilter() . '/A/B/#');

        $this->subscribeAndPublish($subscription, $message);
    }

    /**
     * Tests that retained messages can be successfully published.
     *
     * @depends test_send_and_receive_message_qos_level_1
     */
    public function test_retained_message_is_published(): void
    {
        $client = $this->buildClient();
        $subscription = $this->generateSubscription(1);
        $message = new DefaultMessage($subscription->getFilter(), 'retain', 1, true);

        // Listen for messages
        $client->on(
            'message',
            function (Message $receivedMessage) use ($client, $message): void {
                if ($receivedMessage->getPayload() === '') {
                    // clean retained message
                    return;
                }

                $this->addExpectation($message->getTopic(), $receivedMessage->getTopic(), 'Topics should be the same');
                $this->addExpectation($message->getPayload(), $receivedMessage->getPayload(), 'Payloads should be the same');
                $this->addExpectation(true, $receivedMessage->isRetained(), 'Message should be retained');

                // Cleanup retained message on broker
                $client->publish($message->withPayload('')->withQosLevel(1))
                    ->catch(
                        function () {
                            // ignore
                        }
                    )
                    ->finally(
                        function (): void {
                            $this->stopLoop();
                        }
                    );
            }
        );

        $client->connect($this->hostname, $this->port, null, $this->connectTimeout)
            ->then(
                function () use ($client, $subscription, $message): void {
                    // Here we do the reverse - we publish first! (Retained msg)
                    $client->publish($message)
                        // Now we subscribe and listen on the given topic
                        ->then(
                            function () use ($client, $subscription): void {
                                // Subscribe
                                $client->subscribe($subscription)
                                    ->catch(
                                        function (): void {
                                            $this->addFailure('Failed to subscribe to topic.');
                                            $this->stopLoop();
                                        }
                                    );
                            }
                        )
                        ->catch(
                            function (): void {
                                $this->addFailure('Failed to publish to topic.');
                                $this->stopLoop();
                            }
                        );
                }
            )
            ->catch(
                function (): void {
                    $this->addFailure('Failed to connect.');
                    $this->stopLoop();
                }
            );

        $this->startLoop();
        $this->handleExpectations();
    }

    /**
     * Tests that the will of a client can be set successfully.
     *
     * @depends test_send_and_receive_message_qos_level_1
     */
    public function test_client_will_is_set(): void
    {
        $regularClient = $this->buildClient('regular');

        $will = new DefaultMessage(
            self::TOPIC_PREFIX . uniqid('will'),
            'I see you on the other side!',
            1
        );

        // Listen for messages
        $regularClient->on(
            'message',
            function (Message $receivedMessage) use ($will): void {
                $this->addExpectation($will->getTopic(), $receivedMessage->getTopic(), 'Will topics should be the same');
                $this->addExpectation($will->getPayload(), $receivedMessage->getPayload(), 'Will payloads should be the same');
                $this->stopLoop();
            }
        );

        // Connect
        $regularClient->connect($this->hostname, $this->port, null, $this->connectTimeout)
            ->then(
                function () use ($regularClient, $will): void {
                    $regularClient->subscribe(new DefaultSubscription($will->getTopic(), $will->getQosLevel()))
                        ->then(
                            function () use ($will): void {
                                // In order to test that a will is published, we create a
                                // specialised client, which is going to fail its
                                // connection on purpose.
                                $failingClient = $this->buildClient('failing', false);

                                $failingClient->connect($this->hostname, $this->port, (new DefaultConnection())->withWill($will), $this->connectTimeout)
                                    ->then(
                                        static function () use ($failingClient): void {
                                            // NOTE: This is the only way we can force the
                                            // the broker to publish the will.
                                            if ($failingClient->getStream() !== null) {
                                                $failingClient->getStream()->close();
                                            }
                                        }
                                    )
                                    ->catch(
                                        function (): void {
                                            $this->addFailure('Failed to connect client with will.');
                                            $this->stopLoop();
                                        }
                                    );
                            }
                        )
                        ->catch(
                            function (): void {
                                $this->addFailure('Failed to subscribe to will topic.');
                                $this->stopLoop();
                            }
                        );
                }
            )
            ->catch(
                function (): void {
                    $this->addFailure('Failed to connect.');
                    $this->stopLoop();
                }
            );

        $this->startLoop();
        $this->handleExpectations();
    }

    /**
     * Test that client is able to publish and receive messages, multiple times.
     *
     * @depends test_send_and_receive_message_qos_level_0
     */
    public function test_publish_and_receive_multiple_times(): void
    {
        $client = $this->buildClient();
        $subscription = $this->generateSubscription();
        $messages = [
            'Skiffs wave from fights like rough suns.',
            'The cold wench quirky fires the kraken.',
        ];
        $count = 0;

        // Listen for messages
        $client->on(
            'message',
            function (Message $receivedMessage) use ($subscription, $messages, &$count): void {
                $count++;

                $this->addExpectation($subscription->getFilter(), $receivedMessage->getTopic(), 'Topics should be the same');

                $found = false;

                foreach ($messages as $message) {
                    if ($receivedMessage->getPayload() === $message) {
                        $found = true;

                        break;
                    }
                }

                if (! $found) {
                    $this->addFailure('Unexpected payload.');
                }

                // If we receive 2 (or perhaps more), stop...
                if ($count >= 2) {
                    $this->stopLoop();
                }
            }
        );

        // Connect
        $client->connect($this->hostname, $this->port, null, $this->connectTimeout)
            ->then(
                static function () use ($client, $subscription, $messages): void {
                    // Subscribe
                    $client->subscribe($subscription)
                        ->then(
                            static function (Subscription $subscription) use ($client, $messages): void {
                                // Publish message A
                                $client->publish(new DefaultMessage($subscription->getFilter(), $messages[0]));
                                // Publish message B
                                $client->publish(new DefaultMessage($subscription->getFilter(), $messages[1]));
                            }
                        );
                }
            )
            ->catch(
                function (): void {
                    $this->addFailure('Failed to connect.');
                    $this->stopLoop();
                }
            );

        $this->startLoop();
        $this->handleExpectations();
    }

    /**
     * Tests that messages can be published periodically.
     *
     * @depends test_send_and_receive_message_qos_level_0
     */
    public function test_publish_periodically(): void
    {
        $client = $this->buildClient();
        $subscription = $this->generateSubscription();
        $message = new DefaultMessage($subscription->getFilter(), 'periodic');

        // Listen for messages
        $client->on(
            'message',
            function (Message $receivedMessage) use ($message): void {
                $this->addExpectation($message->getTopic(), $receivedMessage->getTopic(), 'Topics should be the same');
                $this->addExpectation($message->getPayload(), $receivedMessage->getPayload(), 'Payloads should be the same');
                $this->stopLoop();
            }
        );

        // Connect
        $client->connect($this->hostname, $this->port, null, $this->connectTimeout)
            ->then(
                function () use ($client, $subscription, $message): void {
                    // Subscribe
                    $client->subscribe($subscription)
                        ->then(
                            function () use ($client, $message): void {
                                // Publish periodically
                                $generator = (static fn (): string => $message->getPayload());

                                $onProgress = function (Message $message): void {
                                    $this->log(
                                        sprintf('Progress: %s => %s', $message->getTopic(), $message->getPayload())
                                    );
                                };

                                $client->publishPeriodically(1, $message, $generator, $onProgress);
                            }
                        );
                }
            )
            ->catch(
                function (): void {
                    $this->addFailure('Failed to connect.');
                    $this->stopLoop();
                }
            );

        $this->startLoop();
        $this->handleExpectations();
    }

    /**
     * Tests that a client can unsubscribe from a topic.
     *
     * @depends test_connect_success
     */
    public function test_unsubscribe(): void
    {
        $client = $this->buildClient();
        $subscription = $this->generateSubscription();

        // Connect
        $client->connect($this->hostname, $this->port, null, $this->connectTimeout)
            ->then(
                function () use ($client, $subscription): void {
                    // Subscribe
                    $client->subscribe($subscription)
                        ->then(
                            function (Subscription $subscription) use ($client): void {
                                // Unsubscribe
                                $client->unsubscribe($subscription)
                                    ->then(
                                        function (Subscription $s) use ($subscription): void {
                                            $this->addExpectation($subscription->getFilter(), $s->getFilter(), 'Filters should be the same');
                                            $this->log(sprintf('Unsubscribe: %s', $s->getFilter()));
                                            $this->stopLoop();
                                        }
                                    );
                            }
                        );
                }
            )
            ->catch(
                function (): void {
                    $this->addFailure('Failed to connect.');
                    $this->stopLoop();
                }
            );

        $this->startLoop();
        $this->handleExpectations();
    }

    /*******************************************************
     * Helpers
     ******************************************************/

    /**
     * Starts the loop.
     */
    private function startLoop(): void
    {
        $this->loop->run();
    }

    /**
     * Stops the loop.
     */
    private function stopLoop(): void
    {
        $this->client->disconnect();
    }

    /**
     * Returns a new subscription.
     */
    private function generateSubscription(int $qosLevel = 0): Subscription
    {
        return new DefaultSubscription(self::TOPIC_PREFIX . uniqid(), $qosLevel);
    }

    /**
     * Logs the given message.
     */
    private function log(string $message, string $clientName = ''): void
    {
        echo date('H:i:s') . ' - ' . $message . ($clientName !== '' ? ' (' . $clientName . ' client)' : '') . PHP_EOL;
    }

    /**
     * Stores an expectation.
     */
    private function addExpectation($expected, $actual, string $message = ''): void
    {
        $this->expectations[] = [$expected, $actual, $message];
    }

    /**
     * Stores a failure.
     */
    private function addFailure(string $message): void
    {
        $this->failures[] = $message;
    }

    /**
     * Validates all expectations and handles failures.
     */
    private function handleExpectations(): void
    {
        foreach ($this->expectations as $expectation) {
            if (is_bool($expectation[0])) {
                if ($expectation[0] === true) {
                    self::assertTrue($expectation[1], $expectation[2]);
                } else {
                    self::assertFalse($expectation[1], $expectation[2]);
                }
            } else {
                self::assertEquals($expectation[0], $expectation[1], $expectation[2]);
            }
        }

        if (! empty($this->failures)) {
            self::fail(implode(' / ', $this->failures));
        }
    }

    /**
     * Returns a new client.
     */
    private function buildClient(string $name = '', bool $isPrimary = true): ReactMqttClient
    {
        $connector = new Connector($this->loop, ['timeout' => false]);

        $client = new ReactMqttClient($connector, $this->loop);

        if ($isPrimary) {
            $this->client = $client;
        }

        $client->on(
            'open',
            function (Connection $connection, ReactMqttClient $client) use ($name): void {
                $this->log(sprintf('Open: %s:%d', $client->getHost(), $client->getPort()), $name);
            }
        );

        $client->on(
            'close',
            function (Connection $connection, ReactMqttClient $client) use ($name, $isPrimary): void {
                $this->log(sprintf('Close: %s:%d', $client->getHost(), $client->getPort()), $name);

                if ($isPrimary) {
                    $this->loop->stop();
                }
            }
        );

        $client->on(
            'connect',
            function (Connection $connection) use ($name): void {
                $this->log(sprintf('Connect: client=%s', $connection->getClientID()), $name);
            }
        );

        $client->on(
            'disconnect',
            function (Connection $connection) use ($name): void {
                $this->log(sprintf('Disconnect: client=%s', $connection->getClientID()), $name);
            }
        );

        $client->on(
            'message',
            function (Message $message) use ($name): void {
                $this->log(
                    sprintf(
                        'Message: %s => %s, QoS=%d, dup=%d, retain=%d',
                        $message->getTopic(),
                        $message->getPayload(),
                        $message->getQosLevel(),
                        $message->isDuplicate(),
                        $message->isRetained()
                    ),
                    $name
                );
            }
        );

        $client->on(
            'publish',
            function (Message $message) use ($name): void {
                $this->log(
                    sprintf(
                        'Publish: %s => %s, QoS=%d, dup=%d, retain=%d',
                        $message->getTopic(),
                        $message->getPayload(),
                        $message->getQosLevel(),
                        $message->isDuplicate(),
                        $message->isRetained()
                    ),
                    $name
                );
            }
        );

        $client->on(
            'subscribe',
            function (Subscription $subscription) use ($name): void {
                $this->log(
                    sprintf(
                        'Subscribe: %s, QoS=%d',
                        $subscription->getFilter(),
                        $subscription->getQosLevel()
                    ),
                    $name
                );
            }
        );

        $client->on(
            'warning',
            function (Exception $e) use ($name): void {
                $this->log(sprintf('Warning: %s', $e->getMessage()), $name);
            }
        );

        $client->on(
            'error',
            function (Exception $e, ReactMqttClient $client) use ($name): void {
                $this->log(sprintf('Error: %s', $e->getMessage()), $name);

                if (! $client->isConnected()) {
                    $this->loop->stop();
                }
            }
        );

        return $client;
    }

    /**
     * Tests that client is able to subscribe to a topic or a topic filter pattern (topic that contains a wild-card)
     * and receive a message when publishing to a topic that matches that pattern.
     *
     * @param Subscription $subscription Topic or pattern used for subscription, e.g. /A/B/C, /A/B/+, /A/B/#
     * @param Message      $message      Topic used for publication, e.g. /A/B/C, /A/B/foo/bar
     */
    private function subscribeAndPublish(Subscription $subscription, Message $message): void
    {
        $client = $this->buildClient();

        // Listen for messages
        $client->on(
            'message',
            function (Message $receivedMessage) use ($message): void {
                $this->addExpectation($message->getTopic(), $receivedMessage->getTopic(), 'Topics should be the same');
                $this->addExpectation($message->getPayload(), $receivedMessage->getPayload(), 'Payloads should be the same');
                $this->stopLoop();
            }
        );

        // Connect
        $client->connect($this->hostname, $this->port, null, $this->connectTimeout)
            ->then(
                function () use ($client, $subscription, $message): void {
                    // Subscribe
                    $client->subscribe($subscription)
                        ->then(
                            function (Subscription $result) use ($client, $subscription, $message): void {
                                $this->addExpectation($subscription->getFilter(), $result->getFilter(), 'Filters should be the same');

                                // Publish
                                $client->publish($message)
                                    ->catch(
                                        function (): void {
                                            $this->addFailure('Failed to publish message.');
                                            $this->stopLoop();
                                        }
                                    );
                            }
                        )
                        ->catch(
                            function (): void {
                                $this->addFailure('Failed to subscribe to topic.');
                                $this->stopLoop();
                            }
                        );
                }
            )
            ->catch(
                function (): void {
                    $this->addFailure('Failed to connect.');
                    $this->stopLoop();
                }
            );

        $this->startLoop();
        $this->handleExpectations();
    }
}
