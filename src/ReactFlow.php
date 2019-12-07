<?php

declare(strict_types=1);

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
    /** @var Packet|null */
    private $packet;
    /** @var bool */
    private $isSilent;

    /**
     * Constructs an instance of this class.
     *
     * @param Flow     $decorated
     * @param Deferred $deferred
     * @param Packet   $packet
     * @param bool     $isSilent
     */
    public function __construct(Flow $decorated, Deferred $deferred, Packet $packet = null, bool $isSilent = false)
    {
        $this->decorated = $decorated;
        $this->deferred = $deferred;
        $this->packet = $packet;
        $this->isSilent = $isSilent;
    }

    public function getCode(): string
    {
        return $this->decorated->getCode();
    }

    public function start()
    {
        $this->packet = $this->decorated->start();

        return $this->packet;
    }

    public function accept(Packet $packet): bool
    {
        return $this->decorated->accept($packet);
    }

    public function next(Packet $packet)
    {
        $this->packet = $this->decorated->next($packet);

        return $this->packet;
    }

    public function isFinished(): bool
    {
        return $this->decorated->isFinished();
    }

    public function isSuccess(): bool
    {
        return $this->decorated->isSuccess();
    }

    public function getResult()
    {
        return $this->decorated->getResult();
    }

    public function getErrorMessage(): string
    {
        return $this->decorated->getErrorMessage();
    }

    /**
     * Returns the associated deferred.
     *
     * @return Deferred
     */
    public function getDeferred(): Deferred
    {
        return $this->deferred;
    }

    /**
     * Returns the current packet.
     *
     * @return Packet|null
     */
    public function getPacket()
    {
        return $this->packet;
    }

    /**
     * Indicates if the flow should emit events.
     *
     * @return bool
     */
    public function isSilent(): bool
    {
        return $this->isSilent;
    }
}
