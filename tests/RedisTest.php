<?php
/**
 *
 *
 * @author Yecheng Fu <cofyc.jackson@gmail.com>
 */

require_once 'Redis/DB.php';

class RedisTest extends PHPUnit_Framework_TestCase {

    protected function setUp() {
        DB::loadConfigFromFile(CO_ROOT_PATH . '/Redis/config.ini');
    }

    /**
     *
     * @dataProvider miscData
     */
    public function testGlobal($key, $val) {
        $this->assertTrue(DB::getInstance()->set($key, $val));
        $this->assertTrue(DB::getInstance()->get($key) === $val);
    }

    public function miscData() {
        $data = array();
        $data[] = array(
            'sitename',
            'thisisanamestring'
        );
        $data[] = array(
            'anothername',
            'anotherstringname'
        );
        return $data;
    }

    /**
     *
     * @dataProvider userData
     */
    public function testSharding($uid, $key_vals) {
        foreach ($key_vals as $key => $val) {
            $this->assertTrue(DB::getInstance($uid)->set($key, $val));
            $this->assertTrue(DB::getInstance($uid)->get($key) === $val);
        }
    }

    public function userData() {
        $data = array();
        $data[] = array(
            1,
            array(
                'name' => 'name1'
            )
        );
        $data[] = array(
            2,
            array(
                'name' => 'name2'
            )
        );
        $data[] = array(
            3,
            array(
                'name' => 'name2'
            )
        );
        return $data;
    }
}