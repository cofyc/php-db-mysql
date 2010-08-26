<?php
/**
 * A simple SQL Query Builder.
 *
 * @author Yecheng Fu <cofyc.jackson@gmail.com>
 */

abstract class DBQueryBuilder {

    const SELECT = 0;

    const UPDATE = 1;

    const DELETE = 2;

    const INSERT = 3;

    private $select = array();

    private $from;

    private $where = array(); /* key => value */

    private $type;

    /**
     *
     * @throws Exception
     * @return string, false on failure
     */
    public function builder() {
        if ($this->type === self::SELECT) {
            $sql = 'SELECT ';
            if (count($this->select) <= 0) {
                throw new Exception();
            }
            $sql .= implode(',', $this->select);
            if (!isset($this->from)) {
                throw new Exception();
            }
            $sql .= ' FROM `' . $this->from . '`';
            // optional
            if (count($this->where) > 0) {
                $sql .= ' WHERE 1 && ';
                foreach ($this->where as $key => $val) {
                    $sql .= sprintf(' `%s` = %s ', $key, $this->quote($val));
                }
            }
        } else if ($this->type === self::UPDATE) {

        } else if ($this->type === self::DELETE) {

        } else if ($this->type === self::INSERT) {

        } else {
            throw new Exception();
        }
        $this->reset();
        return $sql;
    }

    /**
     *
     * @return void
     */
    public function reset() {
        $this->select = array();
        unset($this->from);
        $this->where = array();
        unset($this->type);
    }

    /**
     *
     * @return DB
     */
    public function select() {
        $this->reset();
        $this->type = self::SELECT;
        if (func_num_args() > 0) {
            $args = func_get_args();
            foreach ($args as $arg) {
                if (!is_string($arg)) {
                    throw new Exception();
                }
            }
            $this->select = $args;
        } else {
            $this->select = array(
                '*'
            );
        }
        return $this;
    }

    /**
     *
     * @param string $table
     * @throws Exception
     * @return DB
     */
    public function from($table) {
        if (!is_string($table) || empty($table)) {
            throw new Exception();
        }
        $this->from = $table;
        return $this;
    }

    /**
     *
     * @return DB
     */
    public function where() {
        $args = func_get_args();
        if (func_num_args() === 2) {
            $this->where[$args[0]] = $args[1];
        } else if (func_num_args() === 1 && is_array($args[0])) {
            $this->where = array_merge($this->where, $args[0]);
        } else {
            throw new Exception();
        }

        return $this;
    }

}