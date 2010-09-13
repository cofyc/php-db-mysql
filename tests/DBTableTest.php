<?php
/**
 *
 *
 * @author Yecheng Fu <cofyc.jackson@gmail.com>
 */

class DBTableTest extends PHPUnit_Framework_TestCase {

    /**
     */
    public function test() {
        $objSettingsTable = DBTable::getInstance(array(
            'table' => 'settings',
            'primary_key' => 'id',
        ));
        $setting = array('name' => sha1(mt_rand()), 'value' => sha1(mt_rand()));
        $pri_key = $objSettingsTable->create($setting);
        $setting['id'] = $pri_key;

        $this->assertEquals($setting, $objSettingsTable->read($pri_key));

        $this->assertType('array', $objSettingsTable->read($pri_key - 1));
        $this->assertFalse($objSettingsTable->read($pri_key + 1));
    }
}
