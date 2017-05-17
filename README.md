# net-mqtt-client-react

[![Latest Version on Packagist][ico-version]][link-packagist]
[![Software License][ico-license]](LICENSE.md)
[![Total Downloads][ico-downloads]][link-downloads]

This package provides an asynchronous MQTT client built on the [React socket](https://github.com/reactphp/socket) library. All client methods return a promise which is fulfilled if the operation succeeded or rejected if the operation failed. Incoming messages of subscribed topics are delivered via the "message" event.

## Install

Via composer:

``` bash
$ composer require binsoul/net-mqtt-client-react
```

## Example

Connect to a public broker and run forever.

``` php
<?php

use BinSoul\Net\Mqtt\Client\React\ReactMqttClient;
use BinSoul\Net\Mqtt\Connection;
use BinSoul\Net\Mqtt\DefaultMessage;
use BinSoul\Net\Mqtt\DefaultSubscription;
use BinSoul\Net\Mqtt\Message;
use BinSoul\Net\Mqtt\Subscription;
use React\Socket\DnsConnector;
use React\Socket\TcpConnector;

include 'vendor/autoload.php';

// Setup client
$loop = \React\EventLoop\Factory::create();
$dnsResolverFactory = new \React\Dns\Resolver\Factory();
$connector = new DnsConnector(new TcpConnector($loop), $dnsResolverFactory->createCached('8.8.8.8', $loop));
$client = new ReactMqttClient($connector, $loop);

// Bind to events
$client->on('open', function () use ($client) {
    // Network connection established
    echo sprintf("Open: %s:%s\n", $client->getHost(), $client->getPort());
});

$client->on('close', function () use ($client, $loop) {
    // Network connection closed
    echo sprintf("Close: %s:%s\n", $client->getHost(), $client->getPort());

    $loop->stop();
});

$client->on('connect', function (Connection $connection) {
    // Broker connected
    echo sprintf("Connect: client=%s\n", $connection->getClientID());
});

$client->on('disconnect', function (Connection $connection) {
    // Broker disconnected
    echo sprintf("Disconnect: client=%s\n", $connection->getClientID());
});

$client->on('message', function (Message $message) {
    // Incoming message
    echo 'Message';

    if ($message->isDuplicate()) {
        echo ' (duplicate)';
    }

    if ($message->isRetained()) {
        echo ' (retained)';
    }

    echo ': '.$message->getTopic().' => '.mb_strimwidth($message->getPayload(), 0, 50, '...');
    echo "\n";
});

$client->on('warning', function (\Exception $e) {
    echo sprintf("Warning: %s\n", $e->getMessage());
});

$client->on('error', function (\Exception $e) use ($loop) {
    echo sprintf("Error: %s\n", $e->getMessage());

    $loop->stop();
});

// Connect to broker
$client->connect('test.mosquitto.org')->then(
    function () use ($client) {
        // Subscribe to all topics
        $client->subscribe(new DefaultSubscription('#'))
            ->then(function (Subscription $subscription) {
                echo sprintf("Subscribe: %s\n", $subscription->getFilter());
            })
            ->otherwise(function (\Exception $e) {
                echo sprintf("Error: %s\n", $e->getMessage());
            });

        // Publish humidity once
        $client->publish(new DefaultMessage('sensors/humidity', '55%'))
            ->then(function (Message $message) {
                echo sprintf("Publish: %s => %s\n", $message->getTopic(), $message->getPayload());
            })
            ->otherwise(function (\Exception $e) {
                echo sprintf("Error: %s\n", $e->getMessage());
            });

        // Publish a random temperature every 10 seconds
        $generator = function () {
            return mt_rand(-20, 30);
        };

        $client->publishPeriodically(10, new DefaultMessage('sensors/temperature'), $generator)
            ->progress(function (Message $message) {
                echo sprintf("Publish: %s => %s\n", $message->getTopic(), $message->getPayload());
            })
            ->otherwise(function (\Exception $e) {
                echo sprintf("Error: %s\n", $e->getMessage());
            });
    }
);

$loop->run();
```

## Testing

``` bash
$ composer test
```

## License

The MIT License (MIT). Please see [License File](LICENSE.md) for more information.

[ico-version]: https://img.shields.io/packagist/v/binsoul/net-mqtt-client-react.svg?style=flat-square
[ico-license]: https://img.shields.io/badge/license-MIT-brightgreen.svg?style=flat-square
[ico-downloads]: https://img.shields.io/packagist/dt/binsoul/net-mqtt-client-react.svg?style=flat-square

[link-packagist]: https://packagist.org/packages/binsoul/net-mqtt-client-react
[link-downloads]: https://packagist.org/packages/binsoul/net-mqtt-client-react
[link-author]: https://github.com/binsoul
