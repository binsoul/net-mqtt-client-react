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
    /**
     * Constructs an instance of this class.
     *
     * @param Deferred<mixed> $deferred
     */
    public function __construct(
        private readonly Flow $decorated,
        private readonly Deferred $deferred,
        private ?Packet $packet = null,
        private readonly bool $isSilent = false,
        private readonly bool $forceSingleResult = false
    ) {
    }

    public function getCode(): string
    {
        return $this->decorated->getCode();
    }

    public function start(): ?Packet
    {
        $this->packet = $this->decorated->start();

        return $this->packet;
    }

    public function accept(Packet $packet): bool
    {
        return $this->decorated->accept($packet);
    }

    public function next(Packet $packet): ?Packet
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

    public function getResult(): mixed
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
     * @return Deferred<mixed>
     */
    public function getDeferred(): Deferred
    {
        return $this->deferred;
    }

    /**
     * Returns the current packet.
     */
    public function getPacket(): ?Packet
    {
        return $this->packet;
    }

    /**
     * Indicates if the flow should emit events.
     */
    public function isSilent(): bool
    {
        return $this->isSilent;
    }

    /**
     * Indicates if the flow should force a single result.
     */
    public function forceSingleResult(): bool
    {
        return $this->forceSingleResult;
    }
}
