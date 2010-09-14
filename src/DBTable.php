<?php
/**
 *
 * @author Yecheng Fu <cofyc.jackson@gmail.com>
 */

require_once 'DB.php';

class DBTable {

    /**
     *
     * @var string
     */
    private $table;

    /**
     *
     * @var string
     */
    private $primary_key;

    /**
     *
     * @var DB
     */
    private $db;

    /**
     *
     * array
     *   - <primary_key> => array
     *
     * @var array
     */
    private $entries;

    /**
     *
     * @var array
     */
    private static $instances;

    /**
     *
     * @var array
     */
    private static $config;

    /**
     *
     * @param array $config
     * array
     *   - table => string              // required
     *   - primary_key => string        // required
     * @return DBTable
     */
    private function __construct(array $config) {
        // table
        if (!isset($config['table'])) {
            throw new Exception('table is required');
        }
        $this->table = $config['table'];

        // primary_key
        if (!isset($config['primary_key'])) {
            throw new Exception('primary_key is required');
        }
        $this->primary_key = $config['primary_key'];

        // DB
        $this->db = DB::getInstanceByTableAndShardKey($this->table);
    }

    /**
     *
     * @param array $config
     * @return DBTable
     */
    public static function getInstance(array $config) {
        $args_key_string = serialize($config);
        if (!isset(self::$instances[$args_key_string])) {
            self::$instances[$args_key_string] = new self($config);
        }
        return self::$instances[$args_key_string];
    }

    /**
     *
     * @param array $entry
     * @return integer/string $pri_key, false on failure
     */
    public function create(array $entry) {
        $id = $this->db->insert($this->table)->value($entry)->query()->lastInsertId();
        if (!isset($entry[$this->primary_key])) {
            $entry[$this->primary_key] = $id;
        }
        $this->entries[$entry[$this->primary_key]] = $entry;
        // S IO::cache
        self::xmemcacher()->set($this->getCacheKey($entry[$this->primary_key]), $entry);
        // E IO::cache
        return $entry[$this->primary_key];
    }

    /**
     *
     * @param integer/string $pri_key
     * @return boolean, false on fialure
     */
    public function read($pri_key) {
        if (!isset($this->entries[$pri_key])) {
            // S IO::cache
            $entry = self::xmemcacher()->get($this->getCacheKey($pri_key));
            // E IO::cache

            // S IO::db
            if ($entry === false) {
                $entry = $this->db->select()->from($this->table)->where(array(
                    $this->primary_key => $pri_key
                ))->query()->fetch();
            }
            // E IO::db

            if ($entry) {
                $this->entries[$pri_key] = $entry;
                // S IO::cache
                self::xmemcacher()->set($this->getCacheKey($pri_key), $entry);
                // E IO::cache
            }
        }
        return isset($this->entries[$pri_key]) ? $this->entries[$pri_key] : false;
    }

    /**
     *
     * @param integer/string $pri_key
     * @param array $entry
     * @return boolean, false on failure
     */
    public function update($pri_key, array $entry) {
        try {
            unset($this->entries[$pri_key]);
            // S IO::cache
            if (!self::xmemcacher()->delete($this->getCacheKey($pri_key))) {
                throw new Exception();
            }
            // E IO::cache
            $this->db->update($this->table)->set($entry)->where(array(
                $this->primary_key => $pri_key
            ))->query();
            return true;
        } catch (Exception $e) {
            return false;
        }
    }

    /**
     *
     * @param integer/string $pri_key
     * @return boolean, false on failure
     */
    public function delete($pri_key) {
        try {
            unset($this->entries[$pri_key]);
            // S IO::cache
            if (!self::xmemcacher()->delete($this->getCacheKey($pri_key))) {
                throw new Exception();
            }
            // E IO::cache
        	$this->db->delete($this->table)->where(array(
                $this->primary_key => $pri_key
            ))->query();
            return true;
        } catch (Exception $e) {
            return false;
        }
    }

    /**
     *
     * @throws Exception
     * @return Memcached
     */
    private static function xmemcacher() {
        static $objMemcacher = null;
        if (!isset($objMemcacher)) {
            if (!isset(self::$config['memcaches'])) {
                throw new Exception('no memcaches config');
            }
            $objMemcacher = new Memcached();
            $objMemcacher->addServers(self::$config['memcaches']);
            $objMemcacher->setOption(Memcached::OPT_BINARY_PROTOCOL, true);
        }
        return $objMemcacher;
    }

    /**
     *
     * @param integer/string $pri_key
     * @return string
     */
    private function getCacheKey($pri_key) {
        return sprintf('dbtable/%s/%s', $this->table, $pri_key);
    }

    /**
     *
     * @param array $config
     */
    public static function setConfig(array $config) {
        self::$config = $config;
    }
}