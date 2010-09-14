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
     * @var array
     */
    private $struct_schema;

    /**
     *
     * @var string
     */
    private $primary_key;

    /**
     *
     * @var string
     */
    private $autoincrement_key;

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
    private static $config;

    /**
     *
     * @param string $table
     * @param array $struct_schema
     * @throws Exception
     * @return DBTable
     */
    public function __construct($table, array $struct_schema) {
        // table
        $this->table = $table;

        // parse table struct schema
        $this->struct_schema = $struct_schema;

        foreach ($struct_schema as $field => $schema) {
            if (!isset($schema['type'])) {
                throw new Exception('every field must have type');
            }

            if (isset($schema['primary'])) {
                if (isset($this->primary_key)) {
                    throw new Exception('only one primary key is supported');
                }
                $this->primary_key = $field;
            }

            if (isset($schema['autoincrement'])) {
                if (isset($this->autoincrement_key)) {
                    throw new Exception('only one autoincrement key is supported');
                }
                $this->autoincrement_key = $field;
            }
        }
        if (!isset($this->primary_key)) {
            throw new Exception('table must have one primary key');
        }

        // DB
        $this->db = DB::getInstanceByTableAndShardKey($this->table);
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
                $entry = $this->type_cast($entry);
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

    /**
     *
     * @param array
     * @throws Exception
     * @return array
     */
    private function type_cast(array $entry) {
        $entry_casted = array();
        foreach ($entry as $key => $val) {
            foreach ($this->struct_schema as $field => $schema) {
                if ($key === $field) {
                    switch ($schema['type']) {
                        case 'integer':
                            $entry_casted[$key] = (int)$val;
                            break;
                        case 'string':
                            $entry_casted[$key] = $val;
                        default:
                            break;
                    }
                }
            }
        }
        return $entry_casted;
    }
}
