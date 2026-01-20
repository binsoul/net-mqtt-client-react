<?php

declare(strict_types=1);

namespace BinSoul\Test\Net\Mqtt\Client\React\Unit;

use BinSoul\Net\Mqtt\Client\React\ReactFlow;
use BinSoul\Net\Mqtt\Flow;
use BinSoul\Net\Mqtt\Packet;
use Iterator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\Group;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use React\Promise\Deferred;
use stdClass;

/**
 * Tests the ReactFlow class.
 */
#[Group('unit')]
final class ReactFlowTest extends TestCase
{
    private Flow&MockObject $decoratedFlow;

    private Deferred $deferred;

    protected function setUp(): void
    {
        $this->decoratedFlow = $this->createMock(Flow::class);
        $this->deferred = new Deferred();
    }

    public function test_constructor_stores_decorated_flow(): void
    {
        $this->decoratedFlow->method('getCode')->willReturn('connect');

        $reactFlow = new ReactFlow(
            $this->decoratedFlow,
            $this->deferred
        );

        $this->assertSame('connect', $reactFlow->getCode());
    }

    public function test_constructor_stores_deferred(): void
    {
        $reactFlow = new ReactFlow(
            $this->decoratedFlow,
            $this->deferred
        );

        $this->assertSame($this->deferred, $reactFlow->getDeferred());
    }

    public function test_constructor_stores_initial_packet_when_provided(): void
    {
        $initialPacket = $this->createMock(Packet::class);
        $reactFlow = new ReactFlow(
            $this->decoratedFlow,
            $this->deferred,
            $initialPacket
        );

        $this->assertSame($initialPacket, $reactFlow->getPacket());
    }

    public function test_constructor_stores_null_packet_when_not_provided(): void
    {
        $reactFlow = new ReactFlow(
            $this->decoratedFlow,
            $this->deferred
        );

        $this->assertNotInstanceOf(Packet::class, $reactFlow->getPacket());
    }

    public function test_constructor_stores_is_silent_flag_when_true(): void
    {
        $reactFlow = new ReactFlow(
            $this->decoratedFlow,
            $this->deferred,
            null,
            true
        );

        $this->assertTrue($reactFlow->isSilent());
    }

    public function test_constructor_stores_is_silent_flag_when_false(): void
    {
        $reactFlow = new ReactFlow(
            $this->decoratedFlow,
            $this->deferred,
            null,
            false
        );

        $this->assertFalse($reactFlow->isSilent());
    }

    public function test_constructor_defaults_is_silent_to_false(): void
    {
        $reactFlow = new ReactFlow(
            $this->decoratedFlow,
            $this->deferred
        );

        $this->assertFalse($reactFlow->isSilent());
    }

    public function test_constructor_stores_force_single_result_flag_when_true(): void
    {
        $reactFlow = new ReactFlow(
            $this->decoratedFlow,
            $this->deferred,
            null,
            false,
            true
        );

        $this->assertTrue($reactFlow->forceSingleResult());
    }

    public function test_constructor_stores_force_single_result_flag_when_false(): void
    {
        $reactFlow = new ReactFlow(
            $this->decoratedFlow,
            $this->deferred,
            null,
            false,
            false
        );

        $this->assertFalse($reactFlow->forceSingleResult());
    }

    public function test_constructor_defaults_force_single_result_to_false(): void
    {
        $reactFlow = new ReactFlow(
            $this->decoratedFlow,
            $this->deferred
        );

        $this->assertFalse($reactFlow->forceSingleResult());
    }

    #[DataProvider('provideCodes')]
    public function test_get_code_delegates_to_decorated_flow(string $code): void
    {
        $decoratedFlow = $this->createMock(Flow::class);
        $decoratedFlow->method('getCode')->willReturn($code);

        $reactFlow = new ReactFlow(
            $decoratedFlow,
            $this->deferred
        );

        $this->assertSame($code, $reactFlow->getCode());
    }

    public function test_start_delegates_to_decorated_flow(): void
    {
        $startPacket = $this->createMock(Packet::class);
        $this->decoratedFlow
            ->expects($this->once())
            ->method('start')
            ->willReturn($startPacket);

        $reactFlow = new ReactFlow(
            $this->decoratedFlow,
            $this->deferred
        );

        $result = $reactFlow->start();

        $this->assertSame($startPacket, $result);
    }

    public function test_start_updates_current_packet(): void
    {
        $startPacket = $this->createMock(Packet::class);
        $this->decoratedFlow->method('start')->willReturn($startPacket);

        $reactFlow = new ReactFlow(
            $this->decoratedFlow,
            $this->deferred
        );

        $reactFlow->start();

        $this->assertSame($startPacket, $reactFlow->getPacket());
    }

    public function test_start_can_return_null_packet(): void
    {
        $this->decoratedFlow->method('start')->willReturn(null);

        $reactFlow = new ReactFlow(
            $this->decoratedFlow,
            $this->deferred
        );

        $result = $reactFlow->start();

        $this->assertNotInstanceOf(Packet::class, $result);
        $this->assertNotInstanceOf(Packet::class, $reactFlow->getPacket());
    }

    public function test_accept_delegates_to_decorated_flow(): void
    {
        $incomingPacket = $this->createMock(Packet::class);
        $this->decoratedFlow->expects($this->once())
            ->method('accept')
            ->with($incomingPacket)
            ->willReturn(true);

        $reactFlow = new ReactFlow(
            $this->decoratedFlow,
            $this->deferred
        );

        $result = $reactFlow->accept($incomingPacket);

        $this->assertTrue($result);
    }

    public function test_accept_returns_false_when_decorated_flow_rejects(): void
    {
        $incomingPacket = $this->createMock(Packet::class);
        $this->decoratedFlow->method('accept')
            ->with($incomingPacket)
            ->willReturn(false);

        $reactFlow = new ReactFlow(
            $this->decoratedFlow,
            $this->deferred
        );

        $result = $reactFlow->accept($incomingPacket);

        $this->assertFalse($result);
    }

    public function test_next_delegates_to_decorated_flow(): void
    {
        $incomingPacket = $this->createMock(Packet::class);
        $nextPacket = $this->createMock(Packet::class);
        $this->decoratedFlow->expects($this->once())
            ->method('next')
            ->with($incomingPacket)
            ->willReturn($nextPacket);

        $reactFlow = new ReactFlow(
            $this->decoratedFlow,
            $this->deferred
        );

        $result = $reactFlow->next($incomingPacket);

        $this->assertSame($nextPacket, $result);
    }

    public function test_next_updates_current_packet(): void
    {
        $incomingPacket = $this->createMock(Packet::class);
        $nextPacket = $this->createMock(Packet::class);
        $this->decoratedFlow->method('next')
            ->with($incomingPacket)
            ->willReturn($nextPacket);

        $reactFlow = new ReactFlow(
            $this->decoratedFlow,
            $this->deferred
        );

        $reactFlow->next($incomingPacket);

        $this->assertSame($nextPacket, $reactFlow->getPacket());
    }

    public function test_next_can_return_null_packet(): void
    {
        $incomingPacket = $this->createMock(Packet::class);
        $this->decoratedFlow->method('next')
            ->with($incomingPacket)
            ->willReturn(null);

        $reactFlow = new ReactFlow(
            $this->decoratedFlow,
            $this->deferred
        );

        $result = $reactFlow->next($incomingPacket);

        $this->assertNotInstanceOf(Packet::class, $result);
        $this->assertNotInstanceOf(Packet::class, $reactFlow->getPacket());
    }

    public function test_is_finished_delegates_to_decorated_flow_when_true(): void
    {
        $this->decoratedFlow->method('isFinished')->willReturn(true);

        $reactFlow = new ReactFlow(
            $this->decoratedFlow,
            $this->deferred
        );

        $this->assertTrue($reactFlow->isFinished());
    }

    public function test_is_finished_delegates_to_decorated_flow_when_false(): void
    {
        $this->decoratedFlow->method('isFinished')->willReturn(false);

        $reactFlow = new ReactFlow(
            $this->decoratedFlow,
            $this->deferred
        );

        $this->assertFalse($reactFlow->isFinished());
    }

    public function test_is_success_delegates_to_decorated_flow_when_true(): void
    {
        $this->decoratedFlow->method('isSuccess')->willReturn(true);

        $reactFlow = new ReactFlow(
            $this->decoratedFlow,
            $this->deferred
        );

        $this->assertTrue($reactFlow->isSuccess());
    }

    public function test_is_success_delegates_to_decorated_flow_when_false(): void
    {
        $this->decoratedFlow->method('isSuccess')->willReturn(false);

        $reactFlow = new ReactFlow(
            $this->decoratedFlow,
            $this->deferred
        );

        $this->assertFalse($reactFlow->isSuccess());
    }

    #[DataProvider('provideResults')]
    public function test_get_result_delegates_to_decorated_flow(mixed $result): void
    {
        $decoratedFlow = $this->createMock(Flow::class);
        $decoratedFlow->method('getResult')->willReturn($result);

        $reactFlow = new ReactFlow(
            $decoratedFlow,
            $this->deferred
        );

        $this->assertSame($result, $reactFlow->getResult());
    }

    #[DataProvider('provideErrorMessages')]
    public function test_get_error_message_delegates_to_decorated_flow(string $errorMessage): void
    {
        $decoratedFlow = $this->createMock(Flow::class);
        $decoratedFlow->method('getErrorMessage')->willReturn($errorMessage);

        $reactFlow = new ReactFlow(
            $decoratedFlow,
            $this->deferred
        );

        $this->assertSame($errorMessage, $reactFlow->getErrorMessage());
    }

    public function test_get_deferred_returns_provided_deferred(): void
    {
        $reactFlow = new ReactFlow(
            $this->decoratedFlow,
            $this->deferred
        );

        $this->assertSame($this->deferred, $reactFlow->getDeferred());
    }

    public function test_get_packet_returns_initial_packet(): void
    {
        $initialPacket = $this->createMock(Packet::class);

        $reactFlow = new ReactFlow(
            $this->decoratedFlow,
            $this->deferred,
            $initialPacket
        );

        $this->assertSame($initialPacket, $reactFlow->getPacket());
    }

    public function test_get_packet_returns_null_when_no_packet_set(): void
    {
        $reactFlow = new ReactFlow(
            $this->decoratedFlow,
            $this->deferred
        );

        $this->assertNotInstanceOf(Packet::class, $reactFlow->getPacket());
    }

    public function test_flow_state_transitions_through_lifecycle(): void
    {
        $startPacket = $this->createMock(Packet::class);
        $incomingPacket = $this->createMock(Packet::class);
        $nextPacket = $this->createMock(Packet::class);

        $this->decoratedFlow->method('start')->willReturn($startPacket);
        $this->decoratedFlow->method('accept')->willReturn(true);
        $this->decoratedFlow->method('next')->willReturn($nextPacket);

        $isFinished = false;
        $this->decoratedFlow->method('isFinished')->willReturnCallback(
            static function () use (&$isFinished): bool {
                return $isFinished;
            }
        );

        $reactFlow = new ReactFlow(
            $this->decoratedFlow,
            $this->deferred
        );

        // Start flow
        $packet = $reactFlow->start();
        $this->assertSame($startPacket, $packet);
        $this->assertFalse($reactFlow->isFinished());

        // Accept and process packet
        $this->assertTrue($reactFlow->accept($incomingPacket));
        $packet = $reactFlow->next($incomingPacket);
        $this->assertSame($nextPacket, $packet);

        // Mark as finished
        $isFinished = true;
        $this->assertTrue($reactFlow->isFinished());
    }

    /**
     * @return Iterator<string, array<int, string>>
     */
    public static function provideCodes(): Iterator
    {
        yield 'connect' => ['connect'];

        yield 'disconnect' => ['disconnect'];

        yield 'publish' => ['publish'];

        yield 'subscribe' => ['subscribe'];
    }

    /**
     * @return Iterator<string, array<int, mixed>>
     */
    public static function provideResults(): Iterator
    {
        yield 'string' => ['hello world'];

        yield 'boolean' => [true];

        yield 'null' => [null];

        yield 'integer' => [42];

        yield 'array' => [['key' => 'value']];

        yield 'object' => [new stdClass()];
    }

    /**
     * @return Iterator<string, array<int, string>>
     */
    public static function provideErrorMessages(): Iterator
    {
        yield 'connect' => ['Failed to connect.'];

        yield 'subscribe' => ['Failed to subscribe.'];
    }
}
