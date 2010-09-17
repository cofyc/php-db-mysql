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

        foreach ($struct_schema as $column => $schema) {
            if (!isset($schema['type'])) {
                throw new Exception('every field must have type');
            }

            if (isset($schema['primary'])) {
                if (isset($this->primary_key)) {
                    throw new Exception('only one primary key is supported');
                }
                $this->primary_key = $column;
            }

            if (isset($schema['autoincrement'])) {
                if (isset($this->autoincrement_key)) {
                    throw new Exception('only one autoincrement key is supported');
                }
                $this->autoincrement_key = $column;
            }
        }
        if (!isset($this->primary_key)) {
            throw new Exception('Table must have one primary key');
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
        try {
            $id = $this->db->insert($this->table)->value($entry)->query()->lastInsertId();
            if ($id > 0) {
                if (!isset($this->autoincrement_key)) {
                    throw new Exception('This table has a column with AUTO_INCREMENT attribute, please specify it.');
                }
                $entry[$this->autoincrement_key] = $id;
            }

            if (!isset($entry[$this->primary_key])) {
                throw new Exception('Table must have one primary key.');
            }

            $pri_key = $entry[$this->primary_key];

            try {
                $this->cacheSet($pri_key, $entry);
            } catch (Exception $e) {
                // ignore
            }

            return $pri_key;
        } catch (Exception $e) {
            return false;
        }
    }

    /**
     *
     * @param integer/string $pri_key
     * @return boolean, false on fialure
     */
    public function read($pri_key) {
        try {
            return $this->cacheGet($pri_key);
        } catch (Exception $e) {
            $entry = $this->db->select()->from($this->table)->where(array(
                $this->primary_key => $pri_key
            ))->query()->fetch();
            if ($entry) {
                $entry = $this->typeCast($entry);
                try {
                    $this->cacheSet($pri_key, $entry);
                } catch (Exception $e) {
                    // ignore
                }
                return $entry;
            } else {
                return false;
            }
        }
    }

    /**
     *
     * @param integer/string $pri_key
     * @param array $entry
     * @return boolean, false on failure
     */
    public function update($pri_key, array $entry) {
        try {
            $this->cacheDelete($pri_key);
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
            $this->cacheDelete($pri_key);
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
     * @param integer/string $pri_key
     * @throws Exception
     */
    private function cacheGet($pri_key) {
        if (!isset($this->entries[$pri_key])) {
            $entry = self::xMemcacher()->get($this->getCacheKey($pri_key));
            if ($entry === false) {
                throw new Exception();
            }
            $this->entries[$pri_key] = $entry;
        }

        return $this->entries[$pri_key];
    }

    /**
     *
     * @param integer/string $pri_key
     * @param array $entry
     * @throws Exception
     */
    private function cacheSet($pri_key, array $entry) {
        if (!self::xMemcacher()->set($this->getCacheKey($pri_key), $entry)) {
            throw new Exception();
        }
        $this->entries[$pri_key] = $entry;
    }

    /**
     *
     * @param integer/string $pri_key
     * @throws Exception
     */
    private function cacheDelete($pri_key) {
        if (!self::xMemcacher()->delete($this->getCacheKey($pri_key))) {
            throw new Exception();
        }
        unset($this->entries[$pri_key]);
    }


    /**
     *
     * @throws Exception
     * @return Memcached
     */
    private static function xMemcacher() {
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
    private function typeCast(array $entry) {
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
