<?php
/**
 *
 *
 * @author Yecheng Fu <cofyc.jackson@gmail.com>
 */

require_once '../MySQL/DB.php';

class DBQueryBuilderTest extends PHPUnit_Framework_TestCase {

    /**
     *
     * DB
     */
    private $objDB;

    public function setUp() {
        global $config;
        DB::setConfig($config);
        $this->objDB = DB::factoryByShardClusterIdAndShardId(1, 1);
    }

    public function tearDown() {
    }

    public function test() {
        echo $this->objDB->select()->from('user')->where('uid', 1234)->builder();
    }
}
