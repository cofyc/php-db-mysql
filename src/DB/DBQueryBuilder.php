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

    const TYPE_INVALID = -1;

    const TYPE_SELECT = 0;

    const TYPE_UPDATE = 1;

    const TYPE_DELETE = 2;

    const TYPE_INSERT = 3;

    /**
     *
     * @var integer
     */
    private $type = self::TYPE_INVALID;

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
    private $where = array();

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
     * @var array
     */
    private $set = array();

    /**
     *
     * @throws Exception
     * @return string
     */
    public function builder() {
        if ($this->type === self::TYPE_SELECT) {
            $sql = 'SELECT ';
            if (count($this->select) <= 0) {
                throw new Exception();
            }
            $sql .= implode(',', $this->select);
            if (!isset($this->table)) {
                throw new Exception();
            }
            $sql .= ' FROM `' . $this->table . '`';
            $sql .= $this->builder_where_expr();
        } else if ($this->type === self::TYPE_UPDATE) {
            $sql = 'UPDATE ';
            if (!isset($this->table)) {
                throw new Exception();
            }
            $sql .= '`' . $this->table . '`';
            if (count($this->set) <= 0) {
                throw new Exception('UPDATE:: no data to set');
            }

            $set_arr = array();
            foreach ($this->set as $key => $val) {
                $set_arr[] = sprintf('%s = %s', $this->escape_column($key), $this->quote($val));
            }
            $sql .= ' SET ' . implode(',', $set_arr);
            $sql .= $this->builder_where_expr();
        } else if ($this->type === self::TYPE_DELETE) {
            $sql = 'DELETE FROM';
            if (!isset($this->table)) {
                throw new Exception();
            }
            $sql .= '`' . $this->table . '`';
            $sql .= $this->builder_where_expr();
        } else if ($this->type === self::TYPE_INSERT) {
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
     * @return string
     */
    private function builder_where_expr() {
        $where_expr = '';
        if (!empty($this->where)) {
            $where_expr .= ' WHERE ';
            $expr_strs = array();
            foreach ($this->where as $key => $val) {
                $expr_strs[] .= sprintf('%s = %s', $this->escape_column($key), $this->quote($val));
            }
            $where_expr .= implode(' && ', $expr_strs);
        }
        return $where_expr;
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
    private function reset() {
        $this->type = self::TYPE_INVALID;
        $this->select = array();
        unset($this->table);
        $this->where = array();
        $this->columns = array();
        $this->rows = array();
        $this->set = array();
    }

    /**
     * SELECT Primary
     *
     * @throws Exception
     * @return DB
     */
    public function select() {
        $this->reset();
        $this->type = self::TYPE_SELECT;
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
     * INSERT Primary
     * @param string $table
     * @param array $columns
     * @return DB
     */
    public function insert($table, $columns = null) {
        $this->reset();
        if (!is_string($table)) {
            throw new Exception();
        }

        $this->type = self::TYPE_INSERT;
        $this->table = $table;

        if (isset($columns)) {
            $this->columns = $columns;
        }

        return $this;
    }

    /**
     * UPDATE Primary
     *
     * @param string $table
     * @return DB
     */
    public function update($table) {
        $this->reset();
        if (!is_string($table)) {
            throw new Exception();
        }

        $this->type = self::TYPE_UPDATE;
        $this->table = $table;

        return $this;
    }

    /**
     * DELETE Primary
     *
     * @param string $table
     * @return DB
     */
    public function delete($table) {
        $this->reset();
        if (!is_string($table)) {
            throw new Exception();
        }

        $this->type = self::TYPE_DELETE;
        $this->table = $table;

        return $this;
    }

    /**
     *
     * DBQueryBuilder::set('column', 'value');
     * DBQueryBuilder::set(array(
     *  'column1' => 'value1',
     *  'column2' => 'value2',
     *  // ...
     * ));
     * @return DB
     */
    public function set() {
        $args = func_get_args();
        if (func_num_args() === 2) {
            $this->set[$args[0]] = $args[1];
        } else if (func_num_args() === 1 && is_array($args[0])) {
            $this->set = array_merge($this->set, $args[0]);
        } else {
            throw new Exception();
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