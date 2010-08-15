<?php
/**
 *
 *
 * @author Yecheng Fu <cofyc.jackson@gmail.com>
 */

require_once '../MySQL/DB.php';

class MySQLTest extends PHPUnit_Framework_TestCase {

    /**
     * @dataProvider provider
     */
    public function testInstance($uid, $data) {
        DB::loadConfigFromFile(CO_ROOT_PATH . '/config.ini');
        $objDB = DB::getInstance($uid);
        $sql = 'INSERT INTO `user`
        	( `uid`
        	, `data`
        	) VALUE
        	( ' . $objDB->quote($uid) . '
        	, ' . $objDB->quote($data) . '
        	)
		';
        $this->assertTrue(DB::getInstance($uid)->query($sql));
        $sql = 'SELECT * FROM `user`
        	WHERE `uid` = ' . DB::getInstance($uid)->quote($uid) . '
        	LIMIT 1
        ';
        $result = DB::getInstance($uid)->query($sql);
        $this->assertTrue($result instanceof mysqli_result);
        $row = $result->fetch_assoc();
        $this->assertTrue(is_array($row));
        $this->assertTrue((int)$row['uid'] === $uid);
        $this->assertTrue($row['data'] === $data);
    }

    public function provider() {
        $data = array();
        for ($i = 0; $i < 1000; $i++) {
            $data[] = array(
                mt_rand(1, mt_getrandmax()),
                sha1(mt_rand())
            );
        }

        return $data;
    }
}
