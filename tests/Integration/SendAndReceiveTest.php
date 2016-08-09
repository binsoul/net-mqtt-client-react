<?php

namespace BinSoul\Test\Net\Mqtt\Client\React\Integration;

use BinSoul\Net\Mqtt\Client\React\ReactMqttClient;
use React\Dns\Resolver\Resolver;
use React\EventLoop\Factory as EventLoopFactory;
use React\Dns\Resolver\Factory as DNSResolverFactory;
use React\SocketClient\DnsConnector;
use React\SocketClient\SecureConnector;
use React\SocketClient\TcpConnector;

/**
 * Send And Receive Test
 *
 * @group integration
 *
 * @author Alin Eugen Deac <ade@vestergaardcompany.com>
 * @package BinSoul\Test\Net\Mqtt\Client\React\Integration
 */
class SendAndReceiveTest extends \PHPUnit_Framework_TestCase
{
    /**
     * Name Server
     *
     * @var string
     */
    protected $nameServer = '8.8.8.8';

    /**
     * Hostname
     *
     * @see http://iot.eclipse.org/getting-started
     *
     * @var string
     */
    protected $hostname = 'iot.eclipse.org';

    /**
     * Port
     *
     * 1883, unsecured connection
     * 8883, secure connection
     *
     * @var int
     */
    protected $port = 8883;

    /**
     * The topic
     *
     * @var string
     */
    protected $topicPrefix = 'testing/BinSoul/';

    /**
     * Event loop
     *
     * @var \React\EventLoop\LoopInterface
     */
    protected $loop;

    /**
     * Loop timeout, duration in seconds
     *
     * @var int
     */
    protected $loopTimeout = 10;

    /**
     * DNS Resolver
     *
     * @var Resolver
     */
    protected $resolver;

    /**
     * List of created clients
     *
     * @var ReactMqttClient[]
     */
    protected $clients = [];

    /**
     * State of connector
     *
     * @var bool
     */
    protected $debug = true;

    /**
     * {@inheritdoc}
     */
    public function setUp()
    {
        // Create event loop
        $this->loop = EventLoopFactory::create();

        // DNS Resolver
        $this->resolver = (new DNSResolverFactory())->createCached($this->nameServer, $this->loop);

        // Add loop timeout
        $this->loop->addPeriodicTimer($this->loopTimeout, function(){
            $this->stop();
            $this->fail('Test timeout');
        });
    }

    /**
     * {@inheritdoc}
     */
    public function tearDown()
    {
        unset($this->loop);

        $this->line();
    }

    /*******************************************************
     * Helpers
     ******************************************************/

    public function start()
    {
        $this->loop->run();
    }

    public function stop()
    {
        foreach ($this->clients as $client){
            $client->disconnect();
        }

        $this->loop->stop();
    }

    /**
     * Returns a new topic
     *
     * @return string
     */
    protected function makeTopic()
    {
        return $this->topicPrefix . uniqid();
    }

    /**
     * Writes to the console
     *
     * @param mixed
     */
    protected function output()
    {
        $message = implode(', ', func_get_args());

        echo PHP_EOL . $message;
    }

    /**
     * Outputs a line
     */
    protected function line()
    {
        $this->output(str_repeat('- - ', 25));
    }

    /**
     * Returns a new client
     *
     * @param bool $secureConnector
     * @return ReactMqttClient
     */
    protected function makeClient($secureConnector = true)
    {
        return $this->setupClient(new ReactMqttClient($this->makeConnector($secureConnector), $this->loop));
    }

    /**
     * Returns a new DUMMY client
     *
     * @param bool $secureConnector
     * @return DummyReactMqttClient
     */
    protected function makeDummyClient($secureConnector = true)
    {
        /** @var DummyReactMqttClient $client */
        $client = $this->setupClient(new DummyReactMqttClient($this->makeConnector($secureConnector), $this->loop));
        return $client;
    }

    /**
     * Returns a new connector instance
     *
     * @param bool $secure [optional]
     * @return DnsConnector|SecureConnector
     */
    public function makeConnector($secure = true)
    {
        $connector = new DnsConnector(new TcpConnector($this->loop), $this->resolver);
        if($secure){
            $connector = new SecureConnector($connector, $this->loop);
        }

        return $connector;
    }

    /**
     * Performs some setup on the given client
     *
     * @param ReactMqttClient $client
     * @return ReactMqttClient
     */
    protected function setupClient(ReactMqttClient $client)
    {
        $client->on('warning', function($e){
            $this->output('warning', $e);
        });
        $client->on('error', function($e){
            $this->output('error', $e);
        });

        return $client;
    }

    /*******************************************************
     * Actual tests
     ******************************************************/

    /**
     * @test
     */
    public function canSendAndReceiveMessage()
    {
        $topic = $this->makeTopic();
        $message = 'Hallo World';
        $client = $this->makeClient();

        // Listen for messages
        $client->on('message', function ($topic, $msg, $isDuplicate, $isRetained) use ($message){

            $this->output($topic, $msg, 'isDuplicate', $isDuplicate, 'isRetained', $isRetained);

            $this->assertSame($message, $msg, 'Incorrect message');
            $this->stop();
        });

        // Connect
        $client->connect($this->hostname, $this->port)
            ->then(function(ReactMqttClient $client) use ($topic, $message){

                $this->output('Connected to', $this->hostname);

                // Subscribe
                $client->subscribe($topic)
                ->then(function($topic) use ($client, $message){

                    $this->output('Subscribed to topic', $topic);
                    return $client;
                })

                // Publish
                ->then(function(ReactMqttClient $client) use($topic, $message){

                    // Publish - need to do this here, because we have to
                    // be subscribed in order to receive the message.
                    $client->publish($topic, $message, 1)
                        ->then(function($value) use($topic){

                            $this->output('Published', $topic, $value);

                        });
                });
            });

        $this->start();
    }

    /**
     * @test
     *
     * @depends canSendAndReceiveMessage
     */
    public function publishedMessageIsRetained()
    {
        $topic = $this->makeTopic();
        $message = '[...] free stuff somewhere, at some time, yet not now! [...]';
        $client = $this->makeClient();

        // Listen for messages
        $client->on('message', function ($topic, $msg, $isDuplicate, $isRetained) use ($message){

            $this->output($topic, $msg, 'isDuplicate', $isDuplicate, 'isRetained', $isRetained);

            $this->assertTrue((bool) $isRetained, 'Message should be retained');
            $this->assertSame($message, $msg, 'Incorrect message');
            $this->stop();
        });

        $client->connect($this->hostname, $this->port)
            ->then(function(ReactMqttClient $client) use ($topic, $message){

                $this->output('Connected to', $this->hostname);

                // Here we do the reverse - we publish first! (Retained msg)
                $client->publish($topic, $message, 1, true)
                    ->then(function($value) use($topic, $client){

                        $this->output('Published', $topic, $value);
                        return $client;
                    })

                    // Now we subscribe and listen on the given topic
                    ->then(function (ReactMqttClient $client) use ($topic){
                        // Subscribe
                        $client->subscribe($topic)
                            ->then(function($topic) use ($client){

                                $this->output('Subscribed to topic', $topic);
                                return $client;
                            });
                    });
            });

        $this->start();
    }

    /**
     * @test
     *
     * @depends canSendAndReceiveMessage
     */
    public function willMessageIsPublished()
    {
        // Make a topic
        $topic = $this->makeTopic();
        $willTopic = $this->makeTopic();

        $message = 'Walking on the moon';
        $willMessage = 'I see you on the other side';

        $client = $this->makeClient();

        // Listen for messages
        $client->on('message', function ($topic, $msg, $isDuplicate, $isRetained) use ($willTopic, $willMessage){

            $this->output($topic, $msg, 'isDuplicate', $isDuplicate, 'isRetained', $isRetained);

            if($topic == $willTopic){
                $this->assertSame($willMessage, $msg, 'Incorrect will message');
                $this->stop();
            }
        });

        // Connect
        $client->connect($this->hostname, $this->port)
            ->then(function(ReactMqttClient $client) use ($topic, $willTopic){

                $this->output('Connected to', $this->hostname);

                // Subscribe
                $client->subscribe($topic)
                ->then(function($topic) use ($client){
                    $this->output('Subscribed to topic', $topic);
                    return $client;
                })

                // Subscribe on the "will topic"
                ->then(function(ReactMqttClient $client) use($willTopic){
                    $client->subscribe($willTopic)
                    ->then(function($topic) use ($client){
                        $this->output('Subscribed to topic', $topic);
                        return $client;
                    });
                });

                return $client;
            })
            ->then(function(ReactMqttClient $client) use ($willTopic, $willMessage, $message, $topic){


                // In order to test that a will is published, we create a
                // specialised client, which is going to fail its
                // connection on purpose.
                $failingClient = $this->makeDummyClient();

                $options = [
                    'will' => [
                        'topic' => $willTopic,
                        'message' => $willMessage,
                        'qos' => 1,
                        'retain' => false,
                    ],
                ];

                $failingClient->connect($this->hostname, $this->port, $options)
                ->then(function(DummyReactMqttClient $failingClient) use ($willTopic, $willMessage, $message, $topic){

                    // Publish a message
                    $failingClient->publish($topic, $message, 1)
                    ->then(function($value) use ($failingClient){

                        // Close the client's stream, which should cause
                        // the will to be sent by the broker.
                        $this->loop->addPeriodicTimer(1, function() use ($failingClient){

                            // NOTE: This is the only way we can force the
                            // the broker to publish the will.
                            $failingClient->getStream()->close();
                        });
                    });
                });
            });

        $this->start();
    }
}

/**
 * Class DummyReactMqttClient
 *
 * FOR TESTING ONLY
 *
 * @author Alin Eugen Deac <ade@vestergaardcompany.com>
 */
class DummyReactMqttClient extends ReactMqttClient {

    /**
     * Returns the stream
     *
     * @return \React\Stream\Stream
     */
    public function getStream()
    {
        return $this->stream;
    }
}