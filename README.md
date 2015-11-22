# net-mqtt-client-react

[![Latest Version on Packagist][ico-version]][link-packagist]
[![Software License][ico-license]](LICENSE.md)
[![Total Downloads][ico-downloads]][link-downloads]

This package provides an asynchronous MQTT client built on the [React socket client](https://github.com/reactphp/socket-client). All client methods return a promise which is fulfilled if the operation succeeded or rejected if the operation failed. Incoming messages of subscribed topics are delivered via the "message" event.
 
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
use React\SocketClient\Connector;

include 'vendor/autoload.php';

// Setup client
$loop = React\EventLoop\Factory::create();
$dnsResolverFactory = new React\Dns\Resolver\Factory();
$connector = new Connector($loop, $dnsResolverFactory->createCached('8.8.8.8', $loop));
$client = new ReactMqttClient($connector, $loop);

// Bind to events
$client->on('connect', function () {
    echo "Connected.\n";
});

$client->on('disconnect', function () {
    echo "Disconnected.\n";
});

$client->on('message', function ($topic, $message, $isDuplicate, $isRetained) {
    echo 'Incoming: '.$topic.' => '.mb_strimwidth($message, 0, 50, '...');
    
    if ($isDuplicate) {
        echo ' (duplicate)';
    }

    if ($isRetained) {
        echo ' (retained)';
    }

    echo "\n";
});

$client->on('warning', function (\Exception $e) {
    echo $e->getMessage();
});

$client->on('error', function (\Exception $e) {
    echo $e->getMessage();
    die();
});

// Connect to broker
$client->connect('test.mosquitto.org')->then(
    function (ReactMqttClient $client) {
        // Subscribe to all topics below "sensors"
        $client->subscribe('sensors/#')
            ->then(function ($topic) {
                echo sprintf("Subscribed to topic '%s'.\n", $topic);
            })
            ->otherwise(function (\Exception $e) {
                echo $e->getMessage();
            });

        // Publish humidity once
        $client->publish('sensors/humidity', '55 %', 1)
            ->then(function ($value) {
                echo sprintf("Published message '%s'.\n", $value);
            })
            ->otherwise(function (\Exception $e) {
                echo $e->getMessage();
            });

        // Publish a random temperature every 10 seconds
        $generator = function () {
            return mt_rand(-20, 30);
        };

        $client->publishPeriodically(10, 'sensors/temperature', $generator, 1)
            ->progress(function ($value) {
                echo sprintf("Published message '%s'.\n", $value);
            })
            ->otherwise(function (\Exception $e) {
                echo $e->getMessage();
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
