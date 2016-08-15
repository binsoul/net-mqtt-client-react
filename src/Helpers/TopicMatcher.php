<?php
namespace BinSoul\Net\Mqtt\Client\React\Helpers;

/**
 * Topic Matcher
 *
 * Utility for matching a topic pattern with an actual topic
 *
 * @author Alin Eugen Deac <ade@vestergaardcompany.com>
 * @package BinSoul\Net\Mqtt\Client\React\Helpers
 */
class TopicMatcher
{
    /**
     * Check if the given topic matches the pattern
     *
     * @param string $pattern E.g. A/B/+, A/B/#
     * @param string $topic E.g. A/B/C, A/B/foo/bar/baz
     *
     * @return bool True if topic matches the pattern
     */
    public function matches($pattern, $topic)
    {
        // Created by Steffen (https://github.com/kernelguy)
        $tokens = explode('/', $pattern);
        $re = [];
        $c = count($tokens);
        for ($i=0 ; $i < $c ; $i++) {
            $t = $tokens[$i];
            switch ($t) {
                case '+':
                    $re[] = '[^/#\+]*';
                    break;

                case '#':
                    if ($i == 0) {
                        $re[] = '[^\+\$]*';
                    } else {
                        $re[] = '[^\+]*';
                    }
                    break;

                default:
                    $re[] = str_replace('+', '\+', $t);
                    break;
            }
        }
        $re = implode('/', $re);
        $re = str_replace('$', '\$', $re);
        $re = '^' . $re . '$';
        $result = (preg_match(';' . $re . ';', $topic) === 1);

        return $result;
    }

}