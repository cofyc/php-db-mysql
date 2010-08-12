<?php
/**
 *
 *
 * @link http://github.com/owlient/phpredis
 * @author Yecheng Fu <cofyc.jackson@gmail.com>
 */

class DB {

    /**
     * for global instance
     * @var DB
     */
    private static $instance;

    /**
     * for sharding instances
     */
    private static $instances = array();

    /**
     * Redis For Sharding
     * @var Redis
     */
    private static $objRedisMaster;

    /**
     *
     * @var array
     */
    private static $config = array(
        'core' => array(
            'master' => null
        ),
        'global' => array(
            'master' => null
        ),
        'shards' => array() // <id> => ''
    );



    /**
     *
     * @var Redis
     */
    private $objRedis;

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
     * @param integer $shard_key, optional
     * @return DB
     */
    private function __construct($shard_key = null) {
        if (!isset($shard_key)) {
            $this->host = self::$config['global']['host'];
            $this->port = self::$config['global']['port'];
            return $this;
        }

        // sharding
        if (!is_int($shard_key)) {
            throw new Exception();
        }

        if (!isset(self::$objRedisMaster)) {
            self::$objRedisMaster = new Redis();
            $host = self::$config['core']['master.host'];
            $port = self::$config['core']['master.port'];
            if (!self::$objRedisMaster->connect($host, $port)) {
                throw new Exception('cannot connect to master redis server');
            }
            if (!self::$objRedisMaster->select(1)) {
                throw new Exception('cannot select database');
            }
        }

        $shard_id = self::$objRedisMaster->get($shard_key);
        if ($shard_id === false) {
            $shard_id = array_rand(self::$config['shards']);
            self::$objRedisMaster->set($shard_key, $shard_id);
            // it's important to keep it persistently
            if (!self::$objRedisMaster->save()) {
                throw new Exception('performs a synchronous save unsuccessfully');
            }
        }

        if (isset(self::$config['shards'][$shard_id])) {
            $this->host = self::$config['shards'][$shard_id]['host'];
            $this->port = self::$config['shards'][$shard_id]['port'];
        }
    }

    /**
     *
     * @param integer $shard_key, optional
     * @return DB
     */
    public static function getInstance($shard_key = null) {
        if (isset($shard_key)) {
            if (!isset(self::$instances[$shard_key])) {
                self::$instances[$shard_key] = new self($shard_key);
            }
            return self::$instances[$shard_key];
        } else {
            if (!isset(self::$instance)) {
                self::$instance = new self();
            }
            return self::$instance;
        }

    }

    /**
     *
     * @param string $config_file
     */
    public static function loadConfigFromFile($config_file) {
        $config = @parse_ini_file($config_file, true);
        if ($config === false) {
            throw new Exception();
        }
        foreach ($config as $key => $val) {
            if (strpos($key, 'shard') === 0) {
                $pieces = explode(' ', $key);
                if (count($pieces) === 2 && $pieces[0] === 'shard') {
                    if (!ctype_digit($pieces[1]) || $pieces[1] <= 0) {
                        throw new Exception('shard id should be positive number');
                    }
                    self::$config['shards'][$pieces[1]] = $val;
                }
            } else if ($key === 'core') {
                self::$config['core'] = $val;
            } else if ($key === 'global') {
                self::$config['global'] = $val;
            } else {
                // ignore
            }
        }
    }

    /**
     * A lazy connector.
     *
     * @return void
     */
    private function xconnect() {
        if (!isset($this->objRedis)) {
            $this->objRedis = new Redis();
            if (!$this->objRedis->connect($this->host, $this->port)) {
                $this->objRedis = null;
                throw new Exception(sprintf('failed to connect to server (%s:%d)', $this->host, $this->port));
            }
        }
    }

    /**
     *
     * @param string $key
     * @return string
     */
    public function get($key) {
        $this->xconnect();
        if (!is_string($key)) {
            throw new Exception('key should be string');
        }
        return $this->objRedis->get($key);
    }

    /**
     *
     * @param string $key
     * @param string $val
     */
    public function set($key, $val) {
        $this->xconnect();
        if (!is_string($key)) {
            throw new Exception('key should be string');
        }
        if (!is_string($val)) {
            throw new Exception('val should be string');
        }
        return $this->objRedis->set($key, $val);
    }
}