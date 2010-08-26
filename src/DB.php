<?php
/**
 *
 *
 * @author Yecheng Fu <cofyc.jackson@gmail.com>
 */

require_once 'DB/DBQueryBuilder.php';

class DB extends DBQueryBuilder {

    /**
     *
     * @var array
     */
    private static $instances = array();

    /**
     *
     * @var array
     */
    private static $config;

    /**
     *
     * @var mysqli
     */
    private $link;

    /**
     * Link pool
     *
     * @var array, array of mysqlis
     */
    private static $links = array();

    /**
     *
     * @var string
     */
    private $host;

    /**
     *
     * @var integer
     */
    private $port;

    /**
     *
     * @var string
     */
    private $username;

    /**
     *
     * @var string
     */
    private $passwd;

    /**
     *
     * @var string
     */
    private $dbname;

    /**
     *
     * @var string
     */
    private $link_key;

    /**
     *
     * @var string
     */
    private $sql;

    /**
     *
     * @var mysqli_result
     */
    private $result;

    /**
     *
     * @var Memcached
     */
    private static $objShardingIndexCacher;

    /**
     *
     * @var array
     */
    private static $shard_indices = array();

    /**
     *
     * @param string $dsn
     * @throws Exception
     * @return DB
     */
    private function __construct($dsn) {
        $infos = self::parseDSN($dsn);
        if (!$infos) {
            throw new Exception('dsn format is wrong');
        }

        $this->host = $infos['host'];
        $this->port = $infos['port'];
        $this->username = $infos['username'];
        $this->passwd = $infos['passwd'];
        $this->dbname = $infos['dbname'];
        $this->link_key = $this->host . $this->port;

        return $this;
    }

    /**
     *
     * @param string $dsn
     * @throws Exception
     * @return DB
     */
    private static function factoryByDSN($dsn) {
        return new self($dsn);
    }

    /**
     *
     * @param string $table
     * @param integer $shard_key, optional
     * @throws Exception
     * @return DB
     */
    private static function factoryByTableAndShardKey($table, $shard_key = null) {
        if (!is_string($table)) {
            throw new Exception('table should be string');
        }
        foreach (self::getConfig('global', array()) as $dsn => $tables) {
            if (in_array($table, $tables)) {
                return new self($dsn);
            }
        }

        if (!isset($shard_key)) {
            throw new Exception('table is not in global tables, shard_key must be provided');
        }

        // sharding
        try {
            $shard = self::sharding($table, $shard_key);
        } catch (Exception $e) {
            throw $e;
        }

        return new self($shard['dsn']);
    }

    /**
     *
     * @param integer $shard_cluster_id
     * @param integer $shard_id
     * @throws Exception
     * @return DB
     */
    private static function factoryByShardClusterIDAndShardID($shard_cluster_id, $shard_id) {
        if (!is_int($shard_cluster_id)) {
            throw new Exception();
        }
        if (!is_int($shard_id)) {
            throw new Exception();
        }
        $sharding_clusters = self::getConfig('sharding.clusters');
        if (!isset($sharding_clusters[$shard_cluster_id][$shard_id])) {
            throw new Exception();
        }
        return new self($sharding_clusters[$shard_cluster_id][$shard_id]['dsn']);
    }

    /**
     *
     * @param string $dsn
     * @throws Exception
     * @return DB
     */
    public static function getInstanceByDSN($dsn) {
        return self::getInstance('factoryByDSN', $dsn);
    }

    /**
     *
     * @param string $table
     * @param integer $shard_key, optional
     * @throws Exception
     * @return DB
     */
    public static function getInstanceByTableAndShardKey($table, $shard_key = null) {
        return self::getInstance('factoryByTableAndShardKey', $table, $shard_key);
    }

    /**
     *
     * @param integer $shard_cluster_id
     * @param integer $shard_id
     * @throws Exception
     * @return DB
     */
    public static function getInstanceByShardClusterIDAndShardID($shard_cluster_id, $shard_id) {
        return self::getInstance('factoryByShardClusterIDAndShardID', $shard_cluster_id, $shard_id);
    }

    /**
     *
     * @param string $factory
     * @param $...
     * @return DB
     */
    private static function getInstance($factory) {
        $args = func_get_args();
        array_shift($args);
        $args_serialized = serialize($args);
        if (!isset(self::$instances[$factory][$args_serialized])) {
            self::$instances[$factory][$args_serialized] = call_user_func_array(array(
                'DB',
                $factory
            ), $args);
        }
        return self::$instances[$factory][$args_serialized];
    }

    /**
     *
     * @param string $table
     * @param integer $shard_key
     * @throws Exception
     * @return array $shard
     */
    private static function sharding($table, $shard_key) {
        if (!is_string($table)) {
            throw new Exception();
        }
        if (!is_int($shard_key)) {
            throw new Exception();
        }

        $cluster_id = self::getConfig(sprintf('sharding.tables.%s', $table));
        if (!isset($cluster_id)) {
            throw new Exception('table does not exist in sharding cluster');
        }

        $shards = self::getConfig(sprintf('sharding.clusters.%d', $cluster_id), array());
        if (!isset(self::$shard_indices[$shard_key])) {
            self::xShardingIndexCacher();

            $shard_id = self::$objShardingIndexCacher->get(self::getIndexCacheKey($cluster_id, $shard_key));
            if ($shard_id === false) {
                $objShardingMaster = DB::getInstanceByDSN(self::getConfig(sprintf('sharding.masters.%d', $cluster_id)));

                $row = $objShardingMaster->query("SELECT shard_id, locked FROM index_user WHERE uid = " . (int)$shard_key)->fetch();
                if ($row) {
                    if ($row['locked']) {
                        throw new Exception(sprintf('shard key %d is locked', $shard_key));
                    }
                    $shard_id = (int)$row['shard_id'];
                } else {
                    $shard_id = self::randAllocate($shards);
                    $sql = 'INSERT INTO index_user
                        ( `uid`
                        , `shard_id`
                        ) VALUE
                        ( ' . (int)$shard_key . '
                        , ' . (int)$shard_id . '
                        )
                    ';
                    try {
                        $objShardingMaster->query($sql);
                    } catch (Exception $e) {
                        throw new Exception('failed to insert index');
                    }
                }

                self::$objShardingIndexCacher->set(self::getIndexCacheKey($cluster_id, $shard_key), $shard_id);
            }

            self::$shard_indices[$shard_key] = $shard_id;
        }

        if (!isset($shards[self::$shard_indices[$shard_key]])) {
            throw new Exception('shard id does not exist');
        }

        return $shards[self::$shard_indices[$shard_key]];
    }

    /**
     *
     * @param array $shards
     * @throws Exception
     * @return integer
     */
    private static function randAllocate($shards) {
        if (!is_array($shards)) {
            throw new Exception();
        }
        $total_weight = 0;
        foreach ($shards as $shard_id => $shard) {
            $total_weight += $shard['weight'];
            $shards[$shard_id]['tmp_weight'] = $total_weight;
        }
        $random_weight = mt_rand(0, $total_weight - 1);
        foreach ($shards as $shard_id => $shard) {
            if ($random_weight < $shard['tmp_weight']) {
                return $shard_id;
            }
        }
        throw new Exception('failed');
    }

    /**
     * Index Cache Warm-Up
     *
     * @throws Exception
     * @return array
     * - total: integer
     * - cached: integer
     * - failed: integer
     * - locked: integer
     */
    public static function warmUpIndexCache() {
        $stats = array();

        $masters = self::getConfig('sharding.masters');
        if (!isset($masters)) {
            throw new Exception();
        }

        self::xShardingIndexCacher();

        foreach ($masters as $cluster_id => $dsn) {
            $_stats = array(
                'total' => 0,
                'cached' => 0,
                'failed' => 0,
                'locked' => 0
            );

            $objShardingMaster = DB::getInstanceByDSN($dsn);

            try {
                $objShardingMaster->query('SELECT uid, locked, shard_id FROM index_user');
            } catch (Exception $e) {
                throw new Exception('faild to read index from db');
            }

            while ($row = $objShardingMaster->fetch()) {
                $_stats['total']++;
                if ($row['locked']) {
                    $_stats['locked']++;
                    continue;
                }

                if (!self::$objShardingIndexCacher->set(self::getIndexCacheKey($cluster_id, (int)$row['uid']), $row['shard_id'])) {
                    $_stats['failed']++;
                } else {
                    $_stats['cached']++;
                }
            }

            $stats[$cluster_id] = $_stats;
        }
        return $stats;
    }

    /**
     *
     * @throws Exception
     * @return void
     */
    private static function xShardingIndexCacher() {
        if (!isset(self::$objShardingIndexCacher)) {
            $obj = new Memcached();
            if ($obj->addServers(self::getConfig('sharding.memcaches', array())) && $obj->setOption(Memcached::OPT_BINARY_PROTOCOL, true)) {
                self::$objShardingIndexCacher = $obj;
            } else {
                throw new Exception();
            }
        }
    }

    /**
     * X-Connector
     *
     * @throws Exception
     * @return void
     */
    private function xconnect() {
        $link = $this->_xconnect();
        if ($link === false) {
            throw new Exception();
        }

        if (!$link->select_db($this->dbname)) {
            throw new Exception('select db failed');
        }

        $this->link = $link;
    }

    /**
     *
     * @return mysqli
     */
    private function _xconnect() {
        if (isset(self::$links[$this->link_key])) {
            return self::$links[$this->link_key];
        }

        $link = new mysqli();
        if (!$link->real_connect($this->host, $this->username, $this->passwd, $this->dbname, $this->port, null, MYSQLI_CLIENT_COMPRESS)) {
            throw new Exception('db error (%d): %s', $link->connect_errno, $link->connect_error);
        }

        if (!$link->set_charset(self::getConfig('core.charset', 'utf8'))) {
            throw new Exception('db error (%d): %s', $link->errno, $link->error);
        }

        self::$links[$this->link_key] = $link;

        return $link;
    }

    /**
     *
     * @param string $sql
     * @throws Exception
     * @return DB
     */
    public function query($sql = null) {
        if (!isset($sql)) {
            $sql = $this->builder();
        }
        $this->xconnect();
        if (!is_string($sql)) {
            throw new Exception();
        }
        $result = $this->link->query($sql);
        if ($result === false) {
            throw new Exception('failed to query on db');
        }
        $this->sql = $sql;
        $this->result = $result;
        return $this;
    }

    /**
     *
     * @return boolean
     */
    public function close() {
        if (!isset($this->link)) {
            return true;
        }
        if ($this->link->close() === false) {
            return false;
        }
        unset($this->link);
        unset(self::$links[$this->link_key]);
        return true;
    }

    /**
     *
     * @throws Exception
     */
    public function beginTransaction() {
        $this->xconnect();
        if (!$this->link->autocommit(false)) {
            throw new Exception();
        }
    }

    /**
     *
     * @throws Exception
     */
    public function commit() {
        $this->xconnect();
        if (!$this->link->commit()) {
            throw new Exception('failed to commit');
        }
        if (!$this->link->autocommit(true)) {
            throw new Exception();
        }
    }

    /**
     *
     * @throws Exception
     */
    public function rollBack() {
        $this->xconnect();
        if (!$this->link->rollback()) {
            throw new Exception('failed to rollback');
        }
        if (!$this->link->autocommit(true)) {
            throw new Exception();
        }
    }

    /**
     *
     * @throws Exception
     * @return integer/string
     * @see http://php.net/mysqli
     */
    public function lastInsertId() {
        $this->xconnect();
        return $this->link->insert_id;
    }

    /**
     *
     * @throws Exception
     * @return array, false
     */
    public function fetch() {
        if ($this->result instanceof mysqli_result) {
            $row = $this->result->fetch_assoc();
            if (!$row) {
                $this->result->free();
                $this->result = null;
            }
            return $row;
        } else {
            throw new Exception('result is not a mysqli_result');
        }
    }

    /**
     *
     * @throws Exception
     * @return array
     */
    public function fetchAll() {
        if (!($this->result instanceof mysqli_result)) {
            throw new Exception('result is not a mysqli_result');
        }
        $rows = array();
        while ($row = $this->result->fetch_assoc()) {
            $rows[] = $row;
        }
        $this->result->free();
        $this->result = null;
        return $rows;
    }

    /**
     *
     * @param mixed $value, can be int/float/string
     * @throws Exception
     * @return string
     */
    public function quote($value) {
        if (is_bool($value)) {
            return $value ? '1' : '0';
        } else if (is_int($value)) {
            return (string)$value;
        } else if (is_float($value)) {
            return sprintf('%F', $value);
        } else if (is_string($value)) {
            return "'" . $this->escape($value) . "'";
        } else {
            throw new Exception();
        }
    }

    /**
     *
     * @param string $value
     * @throws Exception
     * @return string
     */
    private function escape($value) {
        if (!is_string($value)) {
            throw new Exception();
        }

        $this->xconnect();

        return $this->link->real_escape_string($value);
    }

    /**
     *
     * @param array $dsn
     * @return array, false on failure
     */
    private static function parseDSN($dsn) {
        $infos = parse_url($dsn);
        if ($infos === false) {
            return false;
        }

        return array(
            'host' => $infos['host'],
            'port' => isset($infos['port']) ? $infos['port'] : 3306,
            'username' => $infos['user'],
            'passwd' => $infos['pass'],
            'dbname' => substr($infos['path'], 1)
        );
    }

    /**
     *
     * @param array $config
     * @throws Exception
     */
    public static function setConfig($config) {
        if (!is_array($config)) {
            throw new Exception();
        }
        if (!isset($config['core'])) {
            throw new Exception();
        }
        self::$config = $config;
    }

    /**
     *
     * @param string $name
     * @param mixed $default, optional, defaults to null
     */
    private static function getConfig($name, $default = NULL) {
        if (!is_string($name)) {
            throw new Exception();
        }
        $sections = explode('.', $name);
        $config = self::$config;
        while ($section = array_shift($sections)) {
            if (isset($config[$section])) {
                $config = $config[$section];
            } else {
                return $default;
            }
        }
        return $config;
    }

    /**
     *
     * @param integer $cluster_id
     * @param integer $shard_key
     * @throws Exception
     * @return string
     */
    private static function getIndexCacheKey($cluster_id, $shard_key) {
        if (!is_int($cluster_id)) {
            throw new Exception();
        }
        if (!is_int($shard_key)) {
            throw new Exception();
        }
        return sprintf('db/sharding/%d/%d', $cluster_id, $shard_key);
    }
}