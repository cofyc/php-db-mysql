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
        return $entry[$this->primary_key];
    }

    /**
     *
     * @param integer/string $pri_key
     * @return boolean, false on fialure
     */
    public function read($pri_key) {
        if (!isset($this->entries[$pri_key])) {
            $entry = $this->db->select()->from($this->table)->where(array(
                $this->primary_key => $pri_key
            ))->query()->fetch();
            if ($entry) {
                $this->entries[$pri_key] = $entry;
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
        // TODO update it in db
        $this->entries[$pri_key] = $entry;
    }

    /**
     *
     * @param integer/string $pri_key
     * @return boolean, false on failure
     */
    public function delete($pri_key) {
        if (isset($this->entries[$pri_key])) {
            unset($this->entries[$pri_key]);
        }
        // TODO delete it in db
    }
}
