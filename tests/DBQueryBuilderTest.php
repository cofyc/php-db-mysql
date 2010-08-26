<?php
/**
 *
 *
 * @author Yecheng Fu <cofyc.jackson@gmail.com>
 */

require_once 'DB.php';

class DBQueryBuilderTest extends PHPUnit_Framework_TestCase {

    /**
     *
     * DB
     */
    private $objDB;

    public function setUp() {
        global $config;
        DB::setConfig($config);
        $this->objDB = DB::getInstanceByShardClusterIDAndShardID(1, 1);
    }

    public function tearDown() {
    }

    public function test() {
        $this->objDB->select()->from('user')->where('uid', 1234)->query();
        $this->objDB->select()->from('user')->where(array('uid' => 1234))->query();
        $this->objDB->select('uid')->from('user')->where(array('uid' => 1234))->query();
    }
}
