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

    public function __construct() {
        parent::__construct();
        $this->objDB = DB::getInstanceByTableAndShardKey('settings');
    }

    public function test() {
        $setting = array(
            'name' => sha1(mt_rand()),
            'value' => sha1(mt_rand()),
        );

        // DML::INSERT
        $settingid = $this->objDB->insert('settings')->value($setting)->query()->lastInsertId();
        $this->assertTrue($settingid > 0);
        $setting['id'] = $settingid;

        // DML::SELECT
        $this->assertEquals($setting, $this->objDB->select()->from('settings')->where('name', $setting['name'])->query()->fetch());
        $this->assertEquals($setting, $this->objDB->select()->from('settings')->where(array('name' => $setting['name'], 'value' => $setting['value']))->query()->fetch());

        // DML::UPDATE
        $updatedname = sha1(mt_rand());
        $this->objDB->update('settings')->set('name', $updatedname)->where('name', $setting['name'])->query();
        $setting['name'] = $updatedname;
        $this->assertEquals($setting, $this->objDB->select()->from('settings')->where('name', $setting['name'])->query()->fetch());
        $this->assertEquals($setting, $this->objDB->select()->from('settings')->where(array('name' => $setting['name'], 'value' => $setting['value']))->query()->fetch());

        // DML::DELETE
        $this->objDB->delete('settings')->where('name', $setting['name'])->where('value', $setting['value'])->query();
        $this->assertNull($this->objDB->select()->from('settings')->where('name', $setting['name'])->query()->fetch());
    }

}
