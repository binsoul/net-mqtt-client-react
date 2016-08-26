<?php

namespace BinSoul\Test\Net\Mqtt\Client\React\Integration;

use BinSoul\Net\Mqtt\Client\React\ReactMqttClient;
use BinSoul\Net\Mqtt\Connection;
use BinSoul\Net\Mqtt\DefaultConnection;
use BinSoul\Net\Mqtt\DefaultMessage;
use BinSoul\Net\Mqtt\DefaultSubscription;
use BinSoul\Net\Mqtt\Message;
use BinSoul\Net\Mqtt\Subscription;
use React\Dns\Resolver\Resolver;
use React\EventLoop\Factory as EventLoopFactory;
use React\Dns\Resolver\Factory as DNSResolverFactory;
use React\EventLoop\LoopInterface;
use React\SocketClient\DnsConnector;
use React\SocketClient\SecureConnector;
use React\SocketClient\TcpConnector;

/**
 * Tests the ReactMqttClient class.
 *
 * @group   integration
 *
 * @author  Alin Eugen Deac <ade@vestergaardcompany.com>
 */
class ReactMqttClientTest extends \PHPUnit_Framework_TestCase
{
    /**
     * Nameserver.
     *
     * @var string
     */
    const NAMESERVER = '8.8.8.8';

    /**
     * Hostname.
     *
     * @see http://iot.eclipse.org/getting-started
     *
     * @var string
     */
    const HOSTNAME = 'iot.eclipse.org';

    /**
     * Port.
     *
     * 1883, unsecured connection
     * 8883, secure connection
     *
     * @var int
     */
    const PORT = 8883;

    /**
     * @var bool
     */
    const SECURE = true;

    /**
     * The topic prefix.
     *
     * @var string
     */
    const TOPIC_PREFIX = 'testing/BinSoul/';

    /**
     * Loop timeout, duration in seconds.
     *
     * @var int
     */
    const MAXIMUM_EXECUTION_TIME = 10;

    /**
     * Event loop.
     *
     * @var LoopInterface
     */
    private $loop;

    /**
     * DNS resolver.
     *
     * @var Resolver
     */
    private $resolver;
    /** @var ReactMqttClient */
    private $client;

    /**
     * {@inheritdoc}
     */
    public function setUp()
    {
        // Create event loop
        $this->loop = EventLoopFactory::create();

        // DNS Resolver
        $this->resolver = (new DNSResolverFactory())->createCached(self::NAMESERVER, $this->loop);

        // Add loop timeout
        $this->loop->addPeriodicTimer(self::MAXIMUM_EXECUTION_TIME, function () {
            $this->loop->stop();
            $this->fail('Test timeout');
        });

        echo 'Test: '.str_replace(['test_', '_'], ['', ' '], $this->getName()).PHP_EOL.PHP_EOL;
    }

    /**
     * {@inheritdoc}
     */
    public function tearDown()
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
    private function startLoop()
    {
        $this->loop->run();
    }

    /**
     * Stops the loop.
     */
    private function stopLoop()
    {
        $this->client->disconnect();
    }

    /**
     * Returns a new subscription.
     *
     * @param int $qosLevel
     *
     * @return Subscription
     */
    private function generateSubscription($qosLevel = 0)
    {
        return new DefaultSubscription(self::TOPIC_PREFIX.uniqid(), $qosLevel);
    }

    /**
     * Logs the given message.
     *
     * @param string $message
     * @param string $clientName
     */
    private function log($message, $clientName = '')
    {
        echo date('H:i:s').' - '.$message.($clientName !== '' ? ' ('.$clientName.' client)' : '').PHP_EOL;
    }

    /**
     * Returns a new client.
     *
     * @param string $name
     * @param bool   $isPrimary
     *
     * @return ReactMqttClient
     */
    private function buildClient($name = '', $isPrimary = true)
    {
        $connector = new DnsConnector(new TcpConnector($this->loop), $this->resolver);
        if (self::SECURE) {
            $connector = new SecureConnector($connector, $this->loop);
        }

        $client = new ReactMqttClient($connector, $this->loop);
        if ($isPrimary) {
            $this->client = $client;
        }

        $client->on('open', function () use ($name) {
            $this->log(sprintf('Open: %s:%d', self::HOSTNAME, self::PORT), $name);
        });

        $client->on('close', function () use ($name, $isPrimary) {
            $this->log(sprintf('Close: %s:%d', self::HOSTNAME, self::PORT), $name);
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

        $client->on('warning', function (\Exception $e) use ($name) {
            $this->log(sprintf('Warning: %s', $e->getMessage()), $name);
        });

        $client->on('error', function (\Exception $e, ReactMqttClient $client) use ($name) {
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
    private function subscribeAndPublish(Subscription $subscription, Message $message)
    {
        $client = $this->buildClient();

        // Listen for messages
        $client->on('message', function (Message $receivedMessage) use ($message) {
            $this->assertSame($message->getTopic(), $receivedMessage->getTopic(), 'Incorrect topic');
            $this->assertSame($message->getPayload(), $receivedMessage->getPayload(), 'Incorrect payload');
            $this->stopLoop();
        });

        // Connect
        $client->connect(self::HOSTNAME, self::PORT)
            ->then(function () use ($client, $subscription, $message) {
                // Subscribe
                $client->subscribe($subscription)
                    ->then(function () use ($client, $message) {
                        // Publish
                        $client->publish($message)
                            ->otherwise(function () {
                                $this->fail('Failed to publish message.');
                                $this->stopLoop();
                            });
                    })
                    ->otherwise(function () {
                        $this->fail('Failed to subscribe to topic.');
                        $this->stopLoop();
                    });
            });

        $this->startLoop();
    }

    /*******************************************************
     * Actual tests
     ******************************************************/

    /**
     * Tests that a client can successfully connect to a broker.
     */
    public function test_connect_success()
    {
        $client = $this->buildClient();
        $client->connect(self::HOSTNAME, self::PORT)
            ->then(function () use ($client) {
                $this->assertTrue($client->isConnected());
                $this->stopLoop();
            })
            ->otherwise(function () use ($client) {
                $this->assertFalse($client->isConnected());
                $this->stopLoop();
            });

        $this->startLoop();
    }

    /**
     * Tests that a client fails to connect to a invalid broker.
     *
     * @depends test_connect_success
     */
    public function test_connect_failure()
    {
        $client = $this->buildClient();
        $client->connect(self::HOSTNAME, 12345, null, 1)
            ->then(function () use ($client) {
                $this->assertTrue($client->isConnected());
                $this->stopLoop();
            })
            ->otherwise(function () use ($client) {
                $this->assertFalse($client->isConnected());
                $this->stopLoop();
            });

        $this->startLoop();
    }

    /**
     * Test that client's connection state is updated correctly when connected.
     *
     * @depends test_connect_success
     */
    public function test_is_connected_when_connect_event_emitted()
    {
        $client = $this->buildClient();

        $client->on('connect', function () use ($client) {
            $this->assertTrue($client->isConnected(), 'Client is should be connected');
            $this->stopLoop();
        });

        $client->connect(self::HOSTNAME, self::PORT)
            ->then(function () use ($client) {
                $this->assertTrue($client->isConnected());
                $this->stopLoop();
            });

        $this->startLoop();
    }

    /**
     * Test that client's connection state is updated correctly when disconnected.
     *
     * @depends test_connect_success
     */
    public function test_is_disconnected_when_disconnect_event_emitted()
    {
        $client = $this->buildClient();

        $client->on('disconnect', function () use ($client) {
            $this->assertFalse($client->isConnected(), 'Client is should be disconnected');
            $this->stopLoop();
        });

        $client->connect(self::HOSTNAME, self::PORT)
            ->then(function () use ($client) {
                $client->disconnect()
                    ->then(function () use ($client) {
                        $this->assertFalse($client->isConnected());
                        $this->stopLoop();
                    });
            });

        $this->startLoop();
    }

    /**
     * Tests that messages can be send and received successfully.
     *
     * @depends test_connect_success
     */
    public function test_send_and_receive_message_qos_level_0()
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
    public function test_send_and_receive_message_qos_level_1()
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
    public function test_send_and_receive_message_qos_level_2()
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
    public function test_can_subscribe_to_single_level_wildcard_filter()
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
    public function test_can_subscribe_to_multi_level_wildcard_filter()
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
    public function test_retained_message_is_published()
    {
        $client = $this->buildClient();
        $subscription = $this->generateSubscription(1);
        $message = new DefaultMessage($subscription->getFilter(), 'retain', 1, true);

        // Listen for messages
        $client->on('message', function (Message $receivedMessage) use ($client, $message) {
            if ($receivedMessage->getPayload() === '') {
                // clean retained message
                return;
            }

            $this->assertSame($message->getTopic(), $receivedMessage->getTopic(), 'Incorrect topic');
            $this->assertSame($message->getPayload(), $receivedMessage->getPayload(), 'Incorrect payload');
            $this->assertTrue($receivedMessage->isRetained(), 'Message should be retained');

            // Cleanup retained message on broker
            $client->publish($message->withPayload('')->withQosLevel(1))
                ->always(function () {
                    $this->stopLoop();
                });
        });

        $client->connect(self::HOSTNAME, self::PORT)
            ->then(function () use ($client, $subscription, $message) {
                // Here we do the reverse - we publish first! (Retained msg)
                $client->publish($message)
                    // Now we subscribe and listen on the given topic
                    ->then(function () use ($client, $subscription) {
                        // Subscribe
                        $client->subscribe($subscription)
                            ->otherwise(function () {
                                $this->fail('Failed to subscribe to topic.');
                                $this->stopLoop();
                            });
                    })->otherwise(function () {
                        $this->fail('Failed to publish to topic.');
                        $this->stopLoop();
                    });
            });

        $this->startLoop();
    }

    /**
     * Tests that the will of a client can be set successfully.
     *
     * @depends test_send_and_receive_message_qos_level_1
     */
    public function test_client_will_is_set()
    {
        $regularClient = $this->buildClient('regular');

        $will = new DefaultMessage(
            self::TOPIC_PREFIX.uniqid('will'),
            'I see you on the other side!',
            1
        );

        // Listen for messages
        $regularClient->on('message', function (Message $receivedMessage) use ($will) {
            $this->assertSame($will->getTopic(), $receivedMessage->getTopic(), 'Incorrect will topic');
            $this->assertSame($will->getPayload(), $receivedMessage->getPayload(), 'Incorrect will message');
            $this->stopLoop();
        });

        // Connect
        $regularClient->connect(self::HOSTNAME, self::PORT)
            ->then(function () use ($regularClient, $will) {
                $regularClient->subscribe(new DefaultSubscription($will->getTopic(), $will->getQosLevel()))
                    ->then(function () use ($will) {
                        // In order to test that a will is published, we create a
                        // specialised client, which is going to fail its
                        // connection on purpose.
                        $failingClient = $this->buildClient('failing', false);

                        $failingClient->connect(self::HOSTNAME, self::PORT, (new DefaultConnection())->withWill($will))
                            ->then(function () use ($failingClient) {
                                // NOTE: This is the only way we can force the
                                // the broker to publish the will.
                                $failingClient->getStream()->close();
                            });
                    })
                    ->otherwise(function () {
                        $this->fail('Failed to subscribe to will topic.');
                        $this->stopLoop();
                    });
            });

        $this->startLoop();
    }

    /**
     * Test that client is able to publish and receive messages, multiple times.
     *
     * @depends test_send_and_receive_message_qos_level_0
     */
    public function test_publish_and_receive_multiple_times()
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
        $client->connect(self::HOSTNAME, self::PORT)
            ->then(function () use ($client, $subscription, $messages) {
                // Subscribe
                $client->subscribe($subscription)
                    ->then(function (Subscription $subscription) use ($client, $messages) {
                        // Publish message A
                        $client->publish(new DefaultMessage($subscription->getFilter(), $messages[0]));
                        // Publish message B
                        $client->publish(new DefaultMessage($subscription->getFilter(), $messages[1]));
                    });
            });

        $this->startLoop();
    }

    /**
     * Tests that messages can be published periodically.
     *
     * @depends test_send_and_receive_message_qos_level_0
     */
    public function test_publish_periodically()
    {
        $client = $this->buildClient();
        $subscription = $this->generateSubscription();
        $message = new DefaultMessage($subscription->getFilter(), 'periodic');

        // Listen for messages
        $client->on('message', function (Message $receivedMessage) use ($message) {
            $this->assertSame($message->getTopic(), $receivedMessage->getTopic(), 'Incorrect topic');
            $this->assertSame($message->getPayload(), $receivedMessage->getPayload(), 'Incorrect payload');
            $this->stopLoop();
        });

        // Connect
        $client->connect(self::HOSTNAME, self::PORT)
            ->then(function () use ($client, $subscription, $message) {
                // Subscribe
                $client->subscribe($subscription)
                    ->then(function () use ($client, $message) {
                        // Publish periodically
                        $generator = function () use ($message) {
                            return $message->getPayload();
                        };

                        $client->publishPeriodically(1, $message, $generator)
                            ->progress(function (Message $message) {
                                $this->log(
                                    sprintf('Progress: %s => %s', $message->getTopic(), $message->getPayload())
                                );
                            });
                    });
            });

        $this->startLoop();
    }

    /**
     * Tests that a client can unsubscribe from a topic.
     *
     * @depends test_connect_success
     */
    public function test_unsubscribe()
    {
        $client = $this->buildClient();
        $subscription = $this->generateSubscription();

        // Connect
        $client->connect(self::HOSTNAME, self::PORT)
            ->then(function () use ($client, $subscription) {
                // Subscribe
                $client->subscribe($subscription)
                    ->then(function (Subscription $subscription) use ($client) {
                        // Unsubscribe
                        $client->unsubscribe($subscription)
                            ->then(function (Subscription $subscription) {
                                $this->log(sprintf('Unsubscribe: %s', $subscription->getFilter()));
                                $this->stopLoop();
                            });
                    });
            });

        $this->startLoop();
    }
}
