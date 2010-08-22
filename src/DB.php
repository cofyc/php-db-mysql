<?php
/**
 *
 *
 * @author Yecheng Fu <cofyc.jackson@gmail.com>
 */

class DB extends PDO {

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
     * @var PDO
     */
    private $link;

    /**
     * Link pool
     *
     * @var array, array of PDOs
     */
    private static $links = array();

    /**
     *
     * @var string
     */
    private $dsn;

    /**
     *
     * @var string
     */
    private $sql;

    /**
     * Current shard cluster id.
     * @var integer
     */
    private static $shard_cluster_id;

    /**
     *
     * @var Memcached
     */
    private static $objShardingIndexCacher;

    /**
     *
     * @var DB
     */
    private static $objShardingMaster;

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
                __CLASS__,
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

        $sharding_tables = self::getConfig('sharding.tables', array());
        if (!isset($sharding_tables[$table])) {
            throw new Exception();
        }
        self::$shard_cluster_id = $sharding_tables[$table];

        $shards = self::getConfig(sprintf('sharding.clusters.%d', self::$shard_cluster_id), array());
        if (!isset(self::$shard_indices[$shard_key])) {
            self::xShardingIndexCacher();

            $shard_id = self::$objShardingIndexCacher->get(self::getIndexCacheKey($shard_key));
            if ($shard_id === false) {
                self::xShardingMaster();

                // try to read from index db
                try {
                    $row = self::$objShardingMaster->query("SELECT shard_id FROM index_user WHERE uid = " . (int)$shard_key)->fetch();
                    if (!$row || !isset($row['shard_id'])) {
                        throw new Exception('failed to get shard id');
                    }
                    $shard_id = (int)$row['shard_id'];
                } catch (Exception $e) {
                    // or random allocates and stores it right now
                    $shard_id = self::randomSharding($shards);
                    $sql = 'INSERT INTO index_user
                    	( `uid`
                    	, `shard_id`
                    	) VALUE
                    	( ' . (int)$shard_key . '
                    	, ' . (int)$shard_id . '
                    	)
    				';
                    try {
                        self::$objShardingMaster->query($sql);
                    } catch (Exception $e) {
                        throw new Exception('failed to insert index');
                    }
                }

                self::$objShardingIndexCacher->set(self::getIndexCacheKey($shard_key), $shard_id);
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
    private static function randomSharding($shards) {
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
     *
     * @throws Exception
     * @return void
     */
    private static function xShardingMaster() {
        if (!self::$shard_cluster_id) {
            throw new Exception();
        }
        $dsn = self::getConfig(sprintf('sharding.masters.%d', self::$shard_cluster_id));
        self::$objShardingMaster = DB::getInstanceByDSN($dsn);
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
     *
     * @param array $config
     * @throws Exception
     */
    public static function setConfig($config) {
        if (!is_array($config)) {
            // TODO more check code?
            throw new Exception();
        }
        self::$config = $config;
    }

    /**
     *
     * @param string $name
     * @param string | null $default, optional, defaults to null
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
     * @param integer $shard_key
     * @throws Exception
     * @return string
     */
    private static function getIndexCacheKey($shard_key) {
        if (!is_int($shard_key)) {
            throw new Exception();
        }
        return sprintf('db/sharding/%d', self::$shard_cluster_id, $shard_key);
    }
}