<?php

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
    private const DEFAULT_HOSTNAME = 'test.mosquitto.org';

    /**
     * Port.
     *
     * 1883, unsecured connection
     * 8883, secure connection
     *
     * @var int
     */
    private const DEFAULT_PORT = 1883;

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
    private const MAXIMUM_EXECUTION_TIME = 10;

    /**
     * Event loop.
     *
     * @var LoopInterface
     */
    private $loop;

    /**
     * @var ReactMqttClient
     */
    private $client;

    /**
     * MQTT server hostname
     *
     * @var string
     */
    private $hostname;

    /**
     * MQTT server hostname
     *
     * @var int
     */
    private $port;

    protected function setUp(): void
    {
        // Create event loop
        $this->loop = Loop::get();

        // Determine hostname
        $this->hostname = $_SERVER['MQTT_HOSTNAME'] ?? self::DEFAULT_HOSTNAME;
        $this->port = $_SERVER['MQTT_PORT'] ?? self::DEFAULT_PORT;

        // Add loop timeout
        $this->loop->addPeriodicTimer(self::MAXIMUM_EXECUTION_TIME, function () {
            $this->loop->stop();
            $this->fail('Test timeout');
        });

        echo 'Test: '.str_replace(['test_', '_'], ['', ' '], $this->getName()).PHP_EOL.PHP_EOL;
    }

    protected function tearDown(): void
    {
        $this->loop->stop();

        echo str_repeat('- - ', 25).PHP_EOL;
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
        return new DefaultSubscription(self::TOPIC_PREFIX.uniqid(), $qosLevel);
    }

    /**
     * Logs the given message.
     */
    private function log(string $message, string $clientName = ''): void
    {
        echo date('H:i:s').' - '.$message.($clientName !== '' ? ' ('.$clientName.' client)' : '').PHP_EOL;
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

        $client->on('open', function () use ($name, $client) {
            $this->log(sprintf('Open: %s:%d', $client->getHost(), $client->getPort()), $name);
        });

        $client->on('close', function () use ($name, $isPrimary, $client) {
            $this->log(sprintf('Close: %s:%d', $client->getHost(), $client->getPort()), $name);
            if ($isPrimary) {
                $this->loop->stop();
            }
        });

        $client->on('connect', function (Connection $connection) use ($name) {
            $this->log(sprintf('Connect: client=%s', $connection->getClientID()), $name);
        });

        $client->on('disconnect', function (Connection $connection) use ($name) {
            $this->log(sprintf('Disconnect: client=%s', $connection->getClientID()), $name);
        });

        $client->on('message', function (Message $message) use ($name) {
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
        });

        $client->on('publish', function (Message $message) use ($name) {
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
        });

        $client->on('subscribe', function (Subscription $subscription) use ($name) {
            $this->log(
                sprintf(
                    'Subscribe: %s, QoS=%d',
                    $subscription->getFilter(),
                    $subscription->getQosLevel()
                ),
                $name
            );
        });

        $client->on('warning', function (Exception $e) use ($name) {
            $this->log(sprintf('Warning: %s', $e->getMessage()), $name);
        });

        $client->on('error', function (Exception $e, ReactMqttClient $client) use ($name) {
            $this->log(sprintf('Error: %s', $e->getMessage()), $name);
            if (!$client->isConnected()) {
                $this->loop->stop();
            }
        });

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
        $messageReceived = false;

        // Listen for messages
        $client->on('message', function (Message $receivedMessage) use ($message, &$messageReceived) {
            $this->assertSame($message->getTopic(), $receivedMessage->getTopic(), 'Incorrect topic');
            $this->assertSame($message->getPayload(), $receivedMessage->getPayload(), 'Incorrect payload');
            $messageReceived = true;
            $this->stopLoop();
        });

        // Connect
        $client->connect($this->hostname, $this->port)
            ->then(function () use ($client, $subscription, $message) {
                // Subscribe
                $client->subscribe($subscription)
                    ->then(function () use ($client, $message) {
                        // Publish
                        $client->publish($message)
                            ->otherwise(function () {
                                $this->fail('Failed to publish message.');
                            })
                            ->done();
                    })
                    ->otherwise(function () {
                        $this->fail('Failed to subscribe to topic.');
                    })
                    ->done();
            });

        $this->startLoop();
        $this->assertTrue($messageReceived);
    }

    /*******************************************************
     * Actual tests
     ******************************************************/

    /**
     * Tests that a client can successfully connect to a broker.
     */
    public function test_connect_success(): void
    {
        $connected = false;
        $client = $this->buildClient();
        $client->connect($this->hostname, $this->port)
            ->done(function () use ($client, &$connected) {
                $this->assertTrue($client->isConnected());
                $connected = true;
                $this->stopLoop();
            });

        $this->startLoop();
        $this->assertTrue($connected);
    }

    /**
     * Tests that a client fails to connect to an invalid broker.
     *
     * @depends test_connect_success
     */
    public function test_connect_failure(): void
    {
        $connected = null;
        $client = $this->buildClient();
        $client->connect($this->hostname, 12345, null, 0.5)
            ->done(
                function () use (&$connected) {
                    $connected = true;
                },
                function () use ($client, &$connected) {
                    $this->assertFalse($client->isConnected());
                    $connected = false;
                    $this->stopLoop();
                }
            );

        $this->startLoop();
        $this->assertFalse($connected);
    }

    /**
     * Test that client's connection state is updated correctly when connected.
     *
     * @depends test_connect_success
     */
    public function test_is_connected_when_connect_event_emitted(): void
    {
        $client = $this->buildClient();

        $connectCount = 0;

        $client->on('connect', function () use ($client, &$connectCount) {
            $this->assertTrue($client->isConnected(), 'Client is should be connected');
            $connectCount++;
            $this->stopLoop();
        });

        $client->connect($this->hostname, $this->port)
            ->then(function () use ($client, &$connectCount) {
                $this->assertTrue($client->isConnected());
                $connectCount++;
            })->always(function () {
                $this->stopLoop();
            })->done();

        $this->startLoop();
        $this->assertEquals(2, $connectCount);
    }

    /**
     * Test that client's connection state is updated correctly when disconnected.
     *
     * @depends test_connect_success
     */
    public function test_is_disconnected_when_disconnect_event_emitted(): void
    {
        $client = $this->buildClient();

        $disconnectCount = 0;
        $client->on('disconnect', function () use ($client, &$disconnectCount) {
            $this->assertFalse($client->isConnected(), 'Client is should be disconnected');
            $disconnectCount++;
            $this->stopLoop();
        });

        $client->connect($this->hostname, $this->port)
            ->done(function () use ($client, &$disconnectCount) {
                $client->disconnect()
                    ->done(function () use ($client, &$disconnectCount) {
                        $this->assertFalse($client->isConnected());
                        $disconnectCount++;
                        $this->stopLoop();
                    });
            });

        $this->startLoop();
        $this->assertEquals(2, $disconnectCount);
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
        $message = new DefaultMessage($subscription->getFilter().'/A/B/foo/C', 'Never vandalize a ship.');
        $subscription = $subscription->withFilter($subscription->getFilter().'/A/B/+/C');

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
        $message = new DefaultMessage($subscription->getFilter().'/A/B/foo/bar/baz/C', 'Never sail a kraken.');
        $subscription = $subscription->withFilter($subscription->getFilter().'/A/B/#');

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
        $messageReceived = false;


        // Listen for messages
        $client->on('message', function (Message $receivedMessage) use ($client, $message, &$messageReceived) {
            if ($receivedMessage->getPayload() === '') {
                // clean retained message
                return;
            }

            $this->assertSame($message->getTopic(), $receivedMessage->getTopic(), 'Incorrect topic');
            $this->assertSame($message->getPayload(), $receivedMessage->getPayload(), 'Incorrect payload');
            $this->assertTrue($receivedMessage->isRetained(), 'Message should be retained');

            $messageReceived = true;

            // Cleanup retained message on broker
            $client->publish($message->withPayload('')->withQosLevel(1))
                ->always(function () {
                    $this->stopLoop();
                })->done();
        });

        $client->connect($this->hostname, $this->port)
            ->done(function () use ($client, $subscription, $message) {
                // Here we do the reverse - we publish first! (Retained msg)
                $client->publish($message)
                    // Now we subscribe and listen on the given topic
                    ->then(function () use ($client, $subscription) {
                        // Subscribe
                        $client->subscribe($subscription)
                            ->done(null, function () {
                                $this->fail('Failed to subscribe to topic.');
                            });
                    })->otherwise(function () {
                        $this->fail('Failed to publish to topic.');
                    })->done();
            });

        $this->startLoop();
        $this->assertTrue($messageReceived);
    }

    /**
     * Tests that the will of a client can be set successfully.
     *
     */
    public function test_client_will_is_set(): void
    {
        $regularClient = $this->buildClient('regular');

        $will = new DefaultMessage(
            self::TOPIC_PREFIX.uniqid('will'),
            'I see you on the other side!',
            1
        );

        $willReceived = false;

        // Listen for messages
        $regularClient->on('message', function (Message $receivedMessage) use ($will, &$willReceived) {
            $this->assertSame($will->getTopic(), $receivedMessage->getTopic(), 'Incorrect will topic');
            $this->assertSame($will->getPayload(), $receivedMessage->getPayload(), 'Incorrect will message');
            $willReceived = true;
            $this->stopLoop();
        });

        // Connect
        $regularClient->connect($this->hostname, $this->port)
            ->done(function () use ($regularClient, $will) {
                $regularClient->subscribe(new DefaultSubscription($will->getTopic(), $will->getQosLevel()))
                    ->then(function () use ($will) {
                        // In order to test that a will is published, we create a
                        // specialised client, which is going to fail its
                        // connection on purpose.
                        $failingClient = $this->buildClient('failing', false);

                        $failingClient->connect($this->hostname, $this->port, (new DefaultConnection())->withWill($will))
                            ->then(static function () use ($failingClient) {
                                // NOTE: This is the only way we can force the
                                // the broker to publish the will.
                                $failingClient->getStream()->close();
                            });
                    })
                    ->otherwise(function () {
                        $this->fail('Failed to subscribe to will topic.');
                    })
                    ->done();
            });

        $this->startLoop();
        $this->assertTrue($willReceived);
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
        $client->on('message', function (Message $receivedMessage) use ($subscription, $messages, &$count) {
            ++$count;

            $this->assertSame($subscription->getFilter(), $receivedMessage->getTopic(), 'Incorrect topic');
            $this->assertContains($receivedMessage->getPayload(), $messages, 'Unknown payload');

            // If we receive 2 (or perhaps more), stop...
            if ($count >= 2) {
                $this->stopLoop();
            }
        });

        // Connect
        $client->connect($this->hostname, $this->port)
            ->done(static function () use ($client, $subscription, $messages) {
                // Subscribe
                $client->subscribe($subscription)
                    ->then(static function (Subscription $subscription) use ($client, $messages) {
                        // Publish message A
                        $client->publish(new DefaultMessage($subscription->getFilter(), $messages[0]));
                        // Publish message B
                        $client->publish(new DefaultMessage($subscription->getFilter(), $messages[1]));
                    });
            });

        $this->startLoop();
        $this->assertGreaterThanOrEqual(2, $count);
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

        $messageCount = 0;

        // Listen for messages
        $client->on('message', function (Message $receivedMessage) use ($message, &$messageCount) {
            $this->assertSame($message->getTopic(), $receivedMessage->getTopic(), 'Incorrect topic');
            $this->assertSame($message->getPayload(), $receivedMessage->getPayload(), 'Incorrect payload');
            $messageCount++;

            if ($messageCount >= 3) {
                $this->stopLoop();
            }
        });

        // Connect
        $client->connect($this->hostname, $this->port)
            ->done(function () use ($client, $subscription, $message) {
                // Subscribe
                $client->subscribe($subscription)
                    ->done(function () use ($client, $message) {
                        // Publish periodically
                        $generator = static function () use ($message) {
                            return $message->getPayload();
                        };

                        $client->publishPeriodically(1, $message, $generator)
                            ->progress(function (Message $message) {
                                $this->log(
                                    sprintf('Progress: %s => %s', $message->getTopic(), $message->getPayload())
                                );
                            })
                            ->done();
                    });
            });

        $this->startLoop();
        $this->assertGreaterThanOrEqual(3, $messageCount);
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

        $unsubscribed = false;

        // Connect
        $client->connect($this->hostname, $this->port)
            ->done(function () use ($client, $subscription, &$unsubscribed) {
                // Subscribe
                $client->subscribe($subscription)
                    ->done(function (Subscription $subscription) use ($client, &$unsubscribed) {
                        // Unsubscribe
                        $client->unsubscribe($subscription)
                            ->done(function (Subscription $s) use ($subscription, &$unsubscribed) {
                                $this->assertEquals($subscription->getFilter(), $s->getFilter());
                                $this->log(sprintf('Unsubscribe: %s', $s->getFilter()));
                                $unsubscribed = true;
                                $this->stopLoop();
                            });
                    });
            });

        $this->startLoop();
        $this->assertTrue($unsubscribed);
    }
}
