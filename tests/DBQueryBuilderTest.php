<?php
/**
 *
 *
 * @author Yecheng Fu <cofyc.jackson@gmail.com>
 */

class DBQueryBuilderTest extends PHPUnit_Framework_TestCase {

    /**
     *
     * DB
     */
    private $objDB;

    public function setUp() {
        $this->objDB = DB::getInstanceByShardClusterIDAndShardID(1, 1);
    }

    public function tearDown() {}

    public function test() {
        // DML::SELECT
        $this->objDB->select()->from('user')->where('uid', 1234)->query();
        $this->objDB->select()->from('user')->where(array(
            'uid' => 1234
        ))->query();
        $this->objDB->select('uid')->from('user')->where(array(
            'uid' => 1234
        ))->query();
        // DML::INSERT
    }
}
