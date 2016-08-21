<?php

namespace BinSoul\Net\Mqtt\Client\React;

use BinSoul\Net\Mqtt\Flow;
use BinSoul\Net\Mqtt\Packet;
use React\Promise\Deferred;

/**
 * Decorates flows with data required for the {@see ReactMqttClient} class.
 */
class ReactFlow implements Flow
{
    /** @var Flow */
    private $decorated;
    /** @var Deferred */
    private $deferred;
    /** @var Packet */
    private $packet;

    /**
     * Constructs an instance of this class.
     *
     * @param Flow     $decorated
     * @param Deferred $deferred
     * @param Packet   $packet
     */
    public function __construct(Flow $decorated, Deferred $deferred, Packet $packet = null)
    {
        $this->decorated = $decorated;
        $this->deferred = $deferred;
        $this->packet = $packet;
    }

    public function getCode()
    {
        return $this->decorated->getCode();
    }

    public function start()
    {
        $this->packet = $this->decorated->start();

        return $this->packet;
    }

    public function accept(Packet $packet)
    {
        return $this->decorated->accept($packet);
    }

    public function next(Packet $packet)
    {
        $this->packet = $this->decorated->next($packet);

        return $this->packet;
    }

    public function isFinished()
    {
        return $this->decorated->isFinished();
    }

    public function isSuccess()
    {
        return $this->decorated->isSuccess();
    }

    public function getResult()
    {
        return $this->decorated->getResult();
    }

    public function getErrorMessage()
    {
        return $this->decorated->getErrorMessage();
    }

    /**
     * Returns the associated deferred.
     *
     * @return Deferred
     */
    public function getDeferred()
    {
        return $this->deferred;
    }

    /**
     * Returns the current packet.
     *
     * @return Packet
     */
    public function getPacket()
    {
        return $this->packet;
    }
}
