<?php

namespace BinSoul\Test\Net\Mqtt\Client\React\Unit\Helpers;

use BinSoul\Net\Mqtt\Client\React\Helpers\TopicMatcher;

/**
 * Class TopicMatcherTest
 *
 * @group       unit
 * @group       helpers
 * @group       topic-matcher
 *
 * @author Alin Eugen Deac <ade@vestergaardcompany.com>
 * @package BinSoul\Test\Net\Mqtt\Client\React\Unit\Helpers
 */
class TopicMatcherTest extends \PHPUnit_Framework_TestCase
{
    /**
     * Instance of the topic matcher utility
     *
     * @var TopicMatcher
     */
    protected $matcher;

    /**
     * {@inheritdoc}
     */
    public function setUp()
    {
        $this->matcher = new TopicMatcher();
    }

    /*************************************************
     * Data Providers
     ************************************************/

    /**
     * Data provider for canMatchTopic test
     *
     * @see test_can_match_topic()
     *
     * @return array
     */
    public function patternsAndTopics()
    {
        // Test cases inspired by (https://github.com/eclipse/mosquitto) package
        // @see https://github.com/eclipse/mosquitto/blob/master/test/broker/03-pattern-matching.py

        return [
            // pattern, topic, expected to match

            // 0
            ['foo/bar', 'foo/bar', true],

            // 1
            ['foo/+', 'foo/bar', true],

            // 2
            ['foo/+/baz', 'foo/bar/baz', true],

            // 3
            ['foo/+/#', 'foo/bar/baz', true],

            // 4
            ['#', 'foo/bar/baz', true],

            ////////////////////////////////////

            // 5
            ['foo/bar', 'foo', false],

            // 6
            ['foo/+', 'foo/bar/baz', false],

            // 7
            ['foo/+/baz', 'foo/bar/bar', false],

            // 8
            ['foo/+/#', 'fo2/bar/baz', false],

            ////////////////////////////////////

            // 9
            ['#', '/foo/bar', true],

            // 10
            ['/#', '/foo/bar', true],

            // 11
            ['/#', 'foo/bar', false],

            ////////////////////////////////////

            // 12
            ['foo//bar', 'foo//bar', true],

            // 13
            ['foo//+', 'foo//bar', true],

            // 14
            ['foo/+/+/baz', 'foo///baz', true],

            // 15
            ['foo/bar/+', 'foo/bar/', true],

            ////////////////////////////////////

            // 16
            ['foo/#', 'foo/', true],

            // 17
            ['foo#', 'foo', false],

            // 18
            ['fo#o/', 'foo', false],

            // 19
            ['foo#', 'fooa', false],

            // 20
            ['foo+', 'foo', false],

            // 21
            ['foo+', 'fooa', false],

            ////////////////////////////////////

            // 22
            ['test/6/#', 'test/3', false],

            // 23
            ['foo/bar', 'foo/bar', true],

            // 24
            ['foo/+', 'foo/bar', true],

            // 25
            ['foo/+/baz', 'foo/bar/baz', true],

            ////////////////////////////////////

            // 26
            ['A/B/+/#', 'A/B/B/C', true],

            ////////////////////////////////////

            // 27
            ['foo/+/#', 'foo/bar/baz', true],

            // 28
            ['#', 'foo/bar/baz', true],

            ////////////////////////////////////

            // 29
            ['$SYS/bar', '$SYS/bar', true],

            // 30
            ['#', '$SYS/bar', false],

            // 31
            ['$BOB/bar', '$SYS/bar', false],

        ];
    }

    /*************************************************
     * Actual tests
     ************************************************/

    /**
     * @test
     *
     * @dataProvider patternsAndTopics
     *
     * @param string $pattern
     * @param string $topic
     * @param bool $expectedResult
     */
    public function test_can_match_topic($pattern, $topic, $expectedResult)
    {
        $result = $this->matcher->matches($pattern, $topic);

        $this->assertSame($expectedResult, $result);
    }
}