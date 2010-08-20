<?php
/**
 *
 *
 * @author Yecheng Fu <cofyc.jackson@gmail.com>
 */

require_once '../MySQL/DB.php';

$config = array(
    'global' => array(
        'master' => 'mysql://root:root@127.0.0.1:3306/dbtest_global'
    ),
	'master' => array(
        'charset' => 'utf8',
        'dsn' => 'mysql://root:root@127.0.0.1:3306/dbtest_shard_index',
        'memcache_host' => '127.0.0.1',
        'memcache_port' => '11211'
    ),
    'shards' => array(
        1 => array(
            'weight' => '10',
            'dsn' => 'mysql://root:root@127.0.0.1:3306/dbtest_shard_1'
        ),
        2 => array(
            'weight' => '0',
            'dsn' => 'mysql://root:root@127.0.0.1:3306/dbtest_shard_2'
        ),
        3 => array(
            'weight' => '30',
            'dsn' => 'mysql://root:root@127.0.0.1:3306/dbtest_shard_3'
        )
    )
);

class DBTest extends PHPUnit_Framework_TestCase {

    public function setUp() {
        global $config;
        DB::setConfig($config);
        DB::startDebug();
    }

    /**
     * @dataProvider newUserDataProvider
     */
    public function testNewInsertAndSelect($uid, $data) {
        $objDB = DB::getInstance($uid);
        $sql = 'INSERT INTO `user`
        	( `uid`
        	, `data`
        	) VALUE
        	( ' . $objDB->quote($uid) . '
        	, ' . $objDB->quote($data) . '
        	)
		';
        $this->assertTrue(DB::getInstance($uid)->query($sql) instanceof DB);
        $sql = 'SELECT * FROM `user`
        	WHERE `uid` = ' . DB::getInstance($uid)->quote($uid) . '
        	LIMIT 1
        ';
        $db = DB::getInstance($uid)->query($sql);
        $this->assertTrue($db instanceof DB);
        $row = $db->fetch();
        $this->assertTrue(is_array($row));
        $this->assertTrue((int)$row['uid'] === $uid);
        $this->assertTrue($row['data'] === $data);
    }

    public function testTransfer() {
        $link = new mysqli();
        $link->real_connect('127.0.0.1', 'root', 'root', 'dbtest_shard_index', null, null, MYSQLI_CLIENT_COMPRESS);
        $result = $link->query('SELECT * FROM index_user');
        while ($row = mysqli_fetch_assoc($result)) {
            $uid = (int)$row['uid'];
            $shard_id = (int)$row['shard_id'];
            $locked = (boolean)$row['locked'];
            if ($locked) {
                continue;
            }
            if ($shard_id === 3) {
                DB::startTransfer($uid);
                try {
                    $this->transferUser($uid, 1);
                    DB::endTransfer($uid, 1);
                } catch (Exception $e) {
                    DB::resetTransfer($uid);
                }
            }
        }
    }

    /**
     *
     * @param integer $uid
     * @param integer $to
     * @throws Exception
     */
    private function transferUser($uid, $to) {
        $objFromDB = DB::getInstance($uid);
        $tables = array(
            'user' => 'uid'
        );
        $objToDB = DB::factoryByShardId($to);
        $sql = 'SELECT * FROM user WHERE uid = ' . $objFromDB->quote($uid);
        $rows = $objFromDB->query($sql)->fetchAll();
        try {
            $objToDB->beginTransaction();
            foreach ($rows as $row) {
                $sql = 'INSERT INTO `user`
                	( `uid`
                	, `data`
                	) VALUE
                	( ' . $objToDB->quote($row['uid']) . '
                	, ' . $objToDB->quote($row['data']) . '
                	)
        		';
                $objToDB->query($sql);
            }
            $objFromDB->beginTransaction();
            $objFromDB->query('DELETE FROM `user` WHERE uid = ' . DB::getInstance($uid)->quote($uid));
            $objFromDB->commit();
            $objToDB->commit();
        } catch (Exception $e) {
            $objFromDB->rollBack();
            $objToDB->rollBack();
        }
    }

    public function newUserDataProvider() {
        $data = array();
        for ($i = 0; $i < 100; $i++) {
            $data[] = array(
                mt_rand(1, mt_getrandmax()),
                sha1(mt_rand())
            );
        }

        return $data;
    }

    public function testWarmUpIndexCacher() {
        DB::warmUpIndexCache();
    }
}
