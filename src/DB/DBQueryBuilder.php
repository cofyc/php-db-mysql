<?php
/**
 * A simple SQL Query Builder.
 *
 * DML
 * * SELECT
 * * INSERT
 * * UPDATE
 * * DELETE
 *
 * @author Yecheng Fu <cofyc.jackson@gmail.com>
 */

abstract class DBQueryBuilder {

    const INVALID = -1;

    const SELECT = 0;

    const UPDATE = 1;

    const DELETE = 2;

    const INSERT = 3;

    /**
     *
     * @var integer
     */
    private $type = self::INVALID;

    /**
     *
     * @var array
     */
    private $select = array();

    /**
     *
     * @var string
     */
    private $table;

    /**
     *
     * @var array
     */
    private $where = array(); /* key => value */

    /**
     *
     * @var array
     */
    private $columns = array();

    /**
     *
     * @var array
     */
    private $rows = array();

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
            if (!isset($this->table)) {
                throw new Exception();
            }
            $sql .= ' FROM `' . $this->table . '`';
            // optional
            if (count($this->where) > 0) {
                $sql .= ' WHERE 1 &&';
                foreach ($this->where as $key => $val) {
                    $sql .= sprintf(' `%s` = %s', $key, $this->quote($val));
                }
            }
        } else if ($this->type === self::UPDATE) {
        } else if ($this->type === self::DELETE) {
        } else if ($this->type === self::INSERT) {
            $sql = 'INSERT';
            if (!isset($this->table)) {
                throw new Exception();
            }
            $sql .= ' INTO `' . $this->table . '`';
            if (count($this->columns) <= 0) {
                $this->columns = array_keys($this->rows[0]);
            }
            $this->columns = array_map('self::escape_column', $this->columns);
            $sql .= ' (' . implode(',', $this->columns) . ')';
            $value_rows = array();
            foreach ($this->rows as $row) {
                $value_rows[] = '(' . implode(',', array_map(array($this, 'quote'), $row)) . ')';
            }
            $sql .= ' VALUES ' . implode(', ', $value_rows);
        } else {
            throw new Exception();
        }
        $this->reset();
        return $sql;
    }

    /**
     *
     * @param string $column
     * @return string
     */
    private static function escape_column($column) {
        return '`' . $column . '`';
    }

    /**
     *
     * @throws Exception
     * @return void
     */
    public function reset() {
        $this->type = self::INVALID;
        $this->select = array();
        unset($this->table);
        $this->where = array();
        $this->columns = array();
        $this->rows = array();
    }

    /**
     * Primary
     *
     * @throws Exception
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
     * Primary
     * @param string $table
     * @param array $columns
     * @return DB
     */
    public function insert($table, $columns = null) {
        $this->reset();
        if (!is_string($table)) {
            throw new Exception();
        }

        $this->type = self::INSERT;
        $this->table = $table;

        if (isset($columns)) {
            $this->columns = $columns;
        }

        return $this;
    }

    /**
     *
     * @param array $row
     * @return DB
     */
    public function value($row) {
        $this->rows[] = $row;

        return $this;
    }

    /**
     *
     * @param array $rows
     * @return DB
     */
    public function values($rows) {
        $this->rows = array_merge($this->rows, $rows);

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
        $this->table = $table;
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