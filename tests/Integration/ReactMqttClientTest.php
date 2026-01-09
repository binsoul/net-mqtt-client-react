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
     * Hostname.
     *
     * @see http://iot.eclipse.org/getting-started
     *
     * @var string
     */
    private const HOSTNAME = 'broker.hivemq.com';

    /**
     * Port.
     *
     * 1883, unsecured connection
     * 8883, secure connection
     *
     * @var int
     */
    private const PORT = 1883;

    /**
     * The timeout for the connection attempt.
     *
     * @var int
     */
    private const CONNECT_TIMEOUT = 30;

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

    private LoopInterface $loop;

    private ReactMqttClient $client;

    protected function setUp(): void
    {
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
        $client->connect(self::HOSTNAME, self::PORT, null, self::CONNECT_TIMEOUT)
            ->then(
                function (?Connection $connection) use ($client): void {
                    self::assertTrue($client->isConnected(), 'Client should be connected');
                    self::assertNotNull($connection, 'Connection should be an object');
                    $this->stopLoop();
                }
            )
            ->catch(
                function () use ($client): void {
                    self::assertFalse($client->isConnected());
                    $this->stopLoop();
                }
            );

        $this->startLoop();
    }

    /**
     * Tests that a client fails to connect to a invalid broker.
     *
     * @depends test_connect_success
     */
    public function test_connect_failure(): void
    {
        $client = $this->buildClient();
        $client->connect(self::HOSTNAME, 12345, null, 1)
            ->then(
                function (?Connection $connection) use ($client): void {
                    self::assertFalse($client->isConnected());
                    self::assertNull($connection);
                    $this->stopLoop();
                }
            )
            ->catch(
                function () use ($client): void {
                    self::assertFalse($client->isConnected());
                    $this->stopLoop();
                }
            );

        $this->startLoop();
    }

    /**
     * Test that attempting to connect when the client is already connected does work.
     */
    public function test_connect_when_already_connected(): void
    {
        $client = $this->buildClient();
        $client->connect(self::HOSTNAME, self::PORT, null, self::CONNECT_TIMEOUT)
            ->then(
                function () use ($client): void {
                    $client->connect(self::HOSTNAME, 12345, null, 1)->then(
                        function (?Connection $connection) use ($client): void {
                            self::assertTrue($client->isConnected(), 'Client should be connected');
                            self::assertNotNull($connection, 'Connection should be an object');
                        }
                    );
                    $this->stopLoop();
                }
            )
            ->catch(
                function () use ($client): void {
                    self::assertFalse($client->isConnected());
                    $this->stopLoop();
                }
            );

        $this->startLoop();
    }

    /**
     * Test that connecting the client twice returns the same promise.
     */
    public function test_connect_twice(): void
    {
        $client = $this->buildClient();
        $promiseA = $client->connect(self::HOSTNAME, 12345, null, 1);
        $promiseB = $client->connect(self::HOSTNAME, 12345, null, 1);

        self::assertSame($promiseA, $promiseB);

        $this->startLoop();
        $this->stopLoop();
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
                self::assertTrue($client->isConnected(), 'Client should be connected');
                self::assertNotNull($connection, 'Connection should be an object');
                $this->stopLoop();
            }
        );

        $client->connect(self::HOSTNAME, self::PORT, null, self::CONNECT_TIMEOUT)
            ->then(
                function () use ($client): void {
                    self::assertTrue($client->isConnected());
                    $this->stopLoop();
                }
            );

        $this->startLoop();
    }

    /**
     * Test that client's disconnect method correctly handles the case where the client is not connected.
     */
    public function test_disconnect_when_not_connected(): void
    {
        $client = $this->buildClient();
        $client->disconnect()->then(
            function (?Connection $connection) use ($client): void {
                self::assertFalse($client->isConnected(), 'Client should be disconnected');
                self::assertNull($connection, 'Connection should be null');
            }
        );
    }

    /**
     * Test that disconnecting a client twice returns the same promise.
     */
    public function test_disconnect_twice(): void
    {
        $client = $this->buildClient();
        $client->connect(self::HOSTNAME, self::PORT, null, self::CONNECT_TIMEOUT)
            ->then(
                function () use ($client): void {
                    $promiseA = $client->disconnect();
                    $promiseB = $client->disconnect();

                    $this->stopLoop();

                    self::assertSame($promiseA, $promiseB);
                }
            );

        $this->startLoop();
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
                self::assertFalse($client->isConnected(), 'Client should be disconnected');
                self::assertNotNull($connection, 'Connection should be an object');
                $this->stopLoop();
            }
        );

        $client->connect(self::HOSTNAME, self::PORT, null, self::CONNECT_TIMEOUT)
            ->then(
                function () use ($client): void {
                    $client->disconnect()
                        ->then(
                            function (?Connection $connection) use ($client): void {
                                self::assertFalse($client->isConnected());
                                self::assertNotNull($connection);
                                $this->stopLoop();
                            }
                        );
                }
            );

        $this->startLoop();
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

                self::assertSame($message->getTopic(), $receivedMessage->getTopic(), 'Incorrect topic');
                self::assertSame($message->getPayload(), $receivedMessage->getPayload(), 'Incorrect payload');
                self::assertTrue($receivedMessage->isRetained(), 'Message should be retained');

                // Cleanup retained message on broker
                $client->publish($message->withPayload('')->withQosLevel(1))
                    ->always(
                        function (): void {
                            $this->stopLoop();
                        }
                    );
            }
        );

        $client->connect(self::HOSTNAME, self::PORT, null, self::CONNECT_TIMEOUT)
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
                                            $this->stopLoop();
                                            $this->fail('Failed to subscribe to topic.');
                                        }
                                    );
                            }
                        )->catch(
                            function (): void {
                                $this->stopLoop();
                                $this->fail('Failed to publish to topic.');
                            }
                        );
                }
            );

        $this->startLoop();
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
                self::assertSame($will->getTopic(), $receivedMessage->getTopic(), 'Incorrect will topic');
                self::assertSame($will->getPayload(), $receivedMessage->getPayload(), 'Incorrect will message');
                $this->stopLoop();
            }
        );

        // Connect
        $regularClient->connect(self::HOSTNAME, self::PORT, null, self::CONNECT_TIMEOUT)
            ->then(
                function () use ($regularClient, $will): void {
                    $regularClient->subscribe(new DefaultSubscription($will->getTopic(), $will->getQosLevel()))
                        ->then(
                            function () use ($will): void {
                                // In order to test that a will is published, we create a
                                // specialised client, which is going to fail its
                                // connection on purpose.
                                $failingClient = $this->buildClient('failing', false);

                                $failingClient->connect(self::HOSTNAME, self::PORT, (new DefaultConnection())->withWill($will), self::CONNECT_TIMEOUT)
                                    ->then(
                                        static function () use ($failingClient): void {
                                            // NOTE: This is the only way we can force the
                                            // the broker to publish the will.
                                            if ($failingClient->getStream() !== null) {
                                                $failingClient->getStream()->close();
                                            }
                                        }
                                    );
                            }
                        )
                        ->catch(
                            function (): void {
                                $this->stopLoop();
                                $this->fail('Failed to subscribe to will topic.');
                            }
                        );
                }
            );

        $this->startLoop();
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

                self::assertSame($subscription->getFilter(), $receivedMessage->getTopic(), 'Incorrect topic');
                self::assertContains($receivedMessage->getPayload(), $messages, 'Unknown payload');

                // If we receive 2 (or perhaps more), stop...
                if ($count >= 2) {
                    $this->stopLoop();
                }
            }
        );

        // Connect
        $client->connect(self::HOSTNAME, self::PORT, null, self::CONNECT_TIMEOUT)
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
            );

        $this->startLoop();
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
                self::assertSame($message->getTopic(), $receivedMessage->getTopic(), 'Incorrect topic');
                self::assertSame($message->getPayload(), $receivedMessage->getPayload(), 'Incorrect payload');
                $this->stopLoop();
            }
        );

        // Connect
        $client->connect(self::HOSTNAME, self::PORT, null, self::CONNECT_TIMEOUT)
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
            );

        $this->startLoop();
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
        $client->connect(self::HOSTNAME, self::PORT, null, self::CONNECT_TIMEOUT)
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
                                            self::assertEquals($subscription->getFilter(), $s->getFilter());
                                            $this->log(sprintf('Unsubscribe: %s', $s->getFilter()));
                                            $this->stopLoop();
                                        }
                                    );
                            }
                        );
                }
            );

        $this->startLoop();
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
                self::assertSame($message->getTopic(), $receivedMessage->getTopic(), 'Incorrect topic');
                self::assertSame($message->getPayload(), $receivedMessage->getPayload(), 'Incorrect payload');
                $this->stopLoop();
            }
        );

        // Connect
        $client->connect(self::HOSTNAME, self::PORT, null, self::CONNECT_TIMEOUT)
            ->then(
                function () use ($client, $subscription, $message): void {
                    // Subscribe
                    $client->subscribe($subscription)
                        ->then(
                            function (Subscription $result) use ($client, $subscription, $message): void {
                                self::assertEquals($subscription->getFilter(), $result->getFilter());

                                // Publish
                                $client->publish($message)
                                    ->catch(
                                        function (): void {
                                            $this->stopLoop();
                                            $this->fail('Failed to publish message.');
                                        }
                                    );
                            }
                        )
                        ->catch(
                            function (): void {
                                $this->stopLoop();
                                $this->fail('Failed to subscribe to topic.');
                            }
                        );
                }
            );

        $this->startLoop();
    }
}
