<?php

namespace BinSoul\Test\Net\Mqtt\Client\React\Integration;

use BinSoul\Net\Mqtt\Client\React\ReactMqttClient;
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

    /**
     * Primary client.
     *
     * @var ReactMqttClient
     */
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
            $this->stopLoop();
            $this->fail('Test timeout');
        });
    }

    /**
     * {@inheritdoc}
     */
    public function tearDown()
    {
        $this->stopLoop();
        $this->loop = null;

        $this->log(str_repeat('- - ', 25));
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
     * Returns a new topic.
     *
     * @return string
     */
    private function generateTopic()
    {
        return self::TOPIC_PREFIX.uniqid('topic', true);
    }

    /**
     * Logs the given message.
     *
     * @param string $message
     * @param string $clientName
     */
    private function log($message, $clientName = '')
    {
        echo $message.($clientName !== '' ? ' ('.$clientName.' client)' : '').PHP_EOL;
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

        $client->on('connect', function () use ($name) {
            $this->log(sprintf('Connect: %s:%d', self::HOSTNAME, self::PORT), $name);
        });

        $client->on('disconnect', function () use ($name, $isPrimary) {
            $this->log('Disconnected', $name);
            if ($isPrimary) {
                $this->loop->stop();
            }
        });

        $client->on('message', function ($topic, $msg, $isDuplicate, $isRetained) use ($name) {
            $this->log(
                sprintf('Message: %s => %s, isDuplicate=%d, isRetained=%d', $topic, $msg, $isDuplicate, $isRetained),
                $name
            );
        });

        $client->on('warning', function (\Exception $e) use ($name) {
            $this->log(sprintf('Warning: %s', $e->getMessage()), $name);
        });

        $client->on('error', function (\Exception $e) use ($name) {
            $this->log(sprintf('Error: %s', $e->getMessage()), $name);
        });

        return $client;
    }

    /**
     * Tests that client is able to subscribe to a topic or a topic filter pattern (topic that contains a wild-card)
     * and receive a message when publishing to a topic that matches that pattern.
     *
     * @param string $subscriptionTopic Topic or pattern used for subscription, e.g. /A/B/C, /A/B/+, /A/B/#
     * @param string $publishTopic      Topic used for publication, e.g. /A/B/C, /A/B/foo/bar
     * @param string $publishMessage    Message to publish
     */
    private function subscribeAndPublish($subscriptionTopic, $publishTopic, $publishMessage, $qosLevel = 0)
    {
        $client = $this->buildClient();

        // Listen for messages
        $client->on('message', function ($receivedTopic, $receivedMessage) use ($publishTopic, $publishMessage) {
            $this->assertSame($publishTopic, $receivedTopic, 'Incorrect topic');
            $this->assertSame($publishMessage, $receivedMessage, 'Incorrect message');

            $this->stopLoop();
        });

        // Connect
        $client->connect(self::HOSTNAME, self::PORT)
            ->then(function (ReactMqttClient $client) use ($subscriptionTopic, $publishTopic, $publishMessage, $qosLevel) {
                // Subscribe
                $client->subscribe($subscriptionTopic)
                    ->then(function ($topic) {
                        $this->log(sprintf('Subscribed: %s', $topic));
                    })
                    // Publish
                    ->then(function () use ($client, $publishTopic, $publishMessage, $qosLevel) {
                        $client->publish($publishTopic, $publishMessage, $qosLevel)
                            ->then(function ($value) use ($publishTopic) {
                                $this->log(sprintf('Published: %s => %s', $publishTopic, $value));
                            });
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
            ->then(function (ReactMqttClient $client) {
                $this->assertTrue($client->isConnected());
                $client->disconnect();
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
     */
    public function test_connect_failure()
    {
        $client = $this->buildClient();
        $client->connect(self::HOSTNAME, 12345, [], 1)
            ->then(function (ReactMqttClient $client) {
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
     * Tests that messages can be send and received successfully with QoS level 0.
     *
     * @depends test_connect_success
     */
    public function test_send_and_receive_message()
    {
        $topic = $this->generateTopic();
        $this->subscribeAndPublish($topic, $topic, 'qos=0', 0);
    }

    /**
     * Tests that messages can be send and received successfully with QoS level 1.
     *
     * @depends test_connect_success
     */
    public function test_send_and_receive_message_qos_level_1()
    {
        $topic = $this->generateTopic();
        $this->subscribeAndPublish($topic, $topic, 'qos=1', 1);
    }

    /**
     * Tests that messages can be send and received successfully with QoS level 2.
     *
     * @depends test_connect_success
     */
    public function test_send_and_receive_message_qos_level_2()
    {
        $topic = $this->generateTopic();
        $this->subscribeAndPublish($topic, $topic, 'qos=2', 2);
    }

    /**
     * Test that client can to subscribe to a single level wildcard filter, e.g. /A/B/+/C.
     *
     * @depends test_connect_success
     */
    public function test_can_subscribe_to_single_level_wildcard_filter()
    {
        $topicBase = $this->generateTopic();
        $subscriptionTopic = $topicBase.'/A/B/+/C';
        $publishTopic = $topicBase.'/A/B/foo/C';

        $message = 'Never vandalize a ship.';

        $this->subscribeAndPublish($subscriptionTopic, $publishTopic, $message);
    }

    /**
     * Test that client can to subscribe to a multi level wildcard filter, e.g. /A/B/#.
     *
     * @depends test_connect_success
     */
    public function test_can_subscribe_to_multi_level_wildcard_filter()
    {
        $topicBase = $this->generateTopic();
        $subscriptionTopic = $topicBase.'/A/B/#';
        $publishTopic = $topicBase.'/A/B/foo/bar/baz/C';

        $message = 'Never sail a kraken.';

        $this->subscribeAndPublish($subscriptionTopic, $publishTopic, $message);
    }

    /**
     * Tests that retained messages can be successfully published.
     *
     * @depends test_send_and_receive_message
     */
    public function test_retained_message_is_published()
    {
        $client = $this->buildClient();
        $topic = $this->generateTopic();
        $message = 'retain';

        // Listen for messages
        $client->on('message', function ($receivedTopic, $receivedMessage, $isDuplicate, $isRetained) use ($topic, $message, $client) {
            $this->assertSame($topic, $receivedTopic, 'Incorrect topic');
            $this->assertSame($message, $receivedMessage, 'Incorrect message');
            $this->assertTrue((bool) $isRetained, 'Message should be retained');

            // Cleanup retained message on broker
            $client->publish($receivedTopic, '', 1, true)
                ->then(function () {
                    $this->stopLoop();
                });
        });

        $client->connect(self::HOSTNAME, self::PORT)
            ->then(function (ReactMqttClient $client) use ($topic, $message) {
                // Here we do the reverse - we publish first! (Retained msg)
                $client->publish($topic, $message, 1, true)
                    ->then(function ($value) use ($topic) {
                        $this->log(sprintf('Published: %s => %s', $topic, $value));
                    })
                    // Now we subscribe and listen on the given topic
                    ->then(function () use ($client, $topic) {
                        // Subscribe
                        $client->subscribe($topic)
                            ->then(function ($topic) {
                                $this->log(sprintf('Subscribed: %s', $topic));
                            });
                    });
            });

        $this->startLoop();
    }

    /**
     * Tests that the will of a client can be set successfully.
     *
     * @depends test_send_and_receive_message
     */
    public function test_client_will_is_set()
    {
        $regularClient = $this->buildClient('regular');

        $willTopic = $this->generateTopic();
        $willMessage = 'I see you on the other side!';

        // Listen for messages
        $regularClient->on('message', function ($receivedTopic, $receivedMessage) use ($willTopic, $willMessage) {
            $this->assertSame($willTopic, $receivedTopic, 'Incorrect will topic');
            $this->assertSame($willMessage, $receivedMessage, 'Incorrect will message');
            $this->stopLoop();
        });

        // Connect
        $regularClient->connect(self::HOSTNAME, self::PORT)
            ->then(function (ReactMqttClient $client) use ($willTopic) {
                $client->subscribe($willTopic)
                    ->then(function ($topic) {
                        $this->log(sprintf('Subscribed: %s', $topic));
                    });
            })
            ->then(function () use ($willTopic, $willMessage) {
                // In order to test that a will is published, we create a
                // specialised client, which is going to fail its
                // connection on purpose.
                $failingClient = $this->buildClient('failing', false);

                $options = [
                    'will' => [
                        'topic' => $willTopic,
                        'message' => $willMessage,
                        'qos' => 1,
                        'retain' => false,
                    ],
                ];

                $failingClient->connect(self::HOSTNAME, self::PORT, $options)
                    ->then(function (ReactMqttClient $client) {
                        // Close the client's stream, which should cause
                        // the will to be sent by the broker.
                        $this->loop->addTimer(1, function () use ($client) {
                            // NOTE: This is the only way we can force the
                            // the broker to publish the will.
                            $client->getStream()->close();
                        });
                    });
            });

        $this->startLoop();
    }

    /**
     * Tests that messages can be published periodically.
     */
    public function test_publish_periodically()
    {
        $client = $this->buildClient();
        $topic = $this->generateTopic();
        $message = 'periodic';

        // Listen for messages
        $client->on('message', function ($receivedTopic, $receivedMessage) use ($topic, $message) {
            $this->assertSame($topic, $receivedTopic, 'Incorrect topic');
            $this->assertSame($message, $receivedMessage, 'Incorrect message');
            $this->stopLoop();
        });

        // Connect
        $client->connect(self::HOSTNAME, self::PORT)
            ->then(function (ReactMqttClient $client) use ($topic, $message) {
                // Subscribe
                $client->subscribe($topic)
                    ->then(function ($topic) {
                        $this->log(sprintf('Subscribed: %s', $topic));
                    })
                    // Publish periodically
                    ->then(function () use ($client, $topic, $message) {
                        $generator = function () use ($message) {
                            return $message;
                        };

                        $client->publishPeriodically(1, $topic, $generator)
                            ->progress(function ($value) use ($topic) {
                                $this->log(sprintf('Published: %s => %s', $topic, $value));
                            });
                    });
            });

        $this->startLoop();
    }

    /**
     * Test that client is able to publish and receive messages, multiple times.
     */
    public function test_publish_and_receive_multiple_times()
    {
        $client = $this->buildClient();
        $topic = $this->generateTopic();
        $qosLevel = 0;
        $messages = [
            'Skiffs wave from fights like rough suns.',
            'The cold wench quirky fires the kraken.',
        ];
        $count = 0;

        // Listen for messages
        $client->on('message', function ($receivedTopic, $receivedMessage) use ($topic, $messages, &$count) {
            ++$count;

            $this->assertSame($topic, $receivedTopic, 'Incorrect topic');
            $this->assertContains($receivedMessage, $messages, 'Unknown message');

            // If we receive 2 (or perhaps more), stop...
            if ($count >= 2) {
                $this->stopLoop();
            }
        });

        // Connect
        $client->connect(self::HOSTNAME, self::PORT)
            ->then(function (ReactMqttClient $client) use ($topic, $messages, $qosLevel) {
                // Subscribe
                $client->subscribe($topic)
                    ->then(function ($topic) {
                        $this->log(sprintf('Subscribed: %s', $topic));
                    })
                    ->then(function () use ($client, $topic, $messages, $qosLevel) {
                        // Publish message A
                        $client->publish($topic, $messages[0], $qosLevel)
                            ->then(function ($value) use ($topic, $client) {
                                $this->log(sprintf('Published: %s => %s', $topic, $value));
                            });

                        // Publish message B
                        $client->publish($topic, $messages[1], $qosLevel)
                            ->then(function ($value) use ($topic) {
                                $this->log(sprintf('Published: %s => %s', $topic, $value));
                            });
                    });
            });

        $this->startLoop();
    }

    /**
     * Tests that a client can unsubscribe fram a topic.
     */
    public function test_unsubscribe()
    {
        $client = $this->buildClient();
        $topic = $this->generateTopic();

        // Connect
        $client->connect(self::HOSTNAME, self::PORT)
            ->then(function (ReactMqttClient $client) use ($topic) {
                // Subscribe
                $client->subscribe($topic)
                    ->then(function ($topic) {
                        $this->log(sprintf('Subscribed: %s', $topic));
                    })
                    // Unsubscribe
                    ->then(function () use ($client, $topic) {
                        $client->unsubscribe($topic)
                            ->then(function ($topic) {
                                $this->log(sprintf('Unsubscribed: %s', $topic));
                                $this->stopLoop();
                            });
                    });
            });

        $this->startLoop();
    }
}