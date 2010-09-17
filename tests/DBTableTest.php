<?php
/**
 *
 *
 * @author Yecheng Fu <cofyc.jackson@gmail.com>
 */

class DBTableTest extends PHPUnit_Framework_TestCase {

    /**
     *
     * @var DBTable
     */
    private $objDBTable;

    public function __construct() {
        $this->objDBTable = new DBTable('settings', array(
            'id' => array(
                'type' => 'integer',
                'primary' => true,
                'autoincrement' => true,
            ),
            'name' => array(
                'type' => 'string',
            ),
            'value' => array(
                'type' => 'string',
            ),
        ));
    }

    public function test() {
        $setting = array('name' => sha1(mt_rand()), 'value' => sha1(mt_rand()));

        // CREATE
        $pri_key = $this->objDBTable->create($setting);
        $setting['id'] = $pri_key;

        // READ
        $this->assertEquals($setting, $this->objDBTable->read($pri_key));

        // UPDATE
        $setting['name'] = sha1(mt_rand());
        $this->objDBTable->update($pri_key, $setting);
        $this->assertEquals($setting, $this->objDBTable->read($pri_key));

        // DELETE
        $this->objDBTable->delete($pri_key);
        $this->assertEquals(false, $this->objDBTable->read($pri_key));

        // PROFILE::PERFORMANCE
        for ($i = 0; $i < 1000; $i++) {
            $this->objDBTable->read($pri_key);
        }
    }

}
