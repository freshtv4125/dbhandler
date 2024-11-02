<?php
class DbHandler {
    private $connection;
    private $stmt;
    private static $instance = null;
    private $transactionCount = 0;
    private $debug = false;

    private function __construct($host, $username, $password, $database = null) {
        try {
            // Connect without database initially if creating new database
            $dsn = "mysql:host={$host}" . ($database ? ";dbname={$database}" : "") . ";charset=utf8mb4";
            $options = [
                PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
                PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
                PDO::ATTR_EMULATE_PREPARES => false,
                PDO::MYSQL_ATTR_INIT_COMMAND => "SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci",
                PDO::ATTR_PERSISTENT => true // Connection pooling
            ];
            
            $this->connection = new PDO($dsn, $username, $password, $options);
        } catch (PDOException $e) {
            throw new Exception("Connection failed: " . $e->getMessage());
        }
    }

    // Enable/disable debug mode
    public function setDebug($enabled = true) {
        $this->debug = $enabled;
        return $this;
    }

    // Database Management Methods
    public function createDatabase($name, $collation = 'utf8mb4_unicode_ci') {
        $sql = sprintf(
            "CREATE DATABASE IF NOT EXISTS %s CHARACTER SET utf8mb4 COLLATE %s",
            $this->escapeIdentifier($name),
            $collation
        );
        return $this->execute($sql);
    }

    public function dropDatabase($name) {
        return $this->execute("DROP DATABASE IF EXISTS " . $this->escapeIdentifier($name));
    }

    public function listDatabases() {
        return $this->query("SHOW DATABASES")->fetchAll(PDO::FETCH_COLUMN);
    }

    // Table Management Methods
    public function createTable($tableName, array $columns, $engine = 'InnoDB') {
        $columnDefinitions = [];
        $primaryKey = null;
        $uniqueKeys = [];
        $indexes = [];
        $foreignKeys = [];

        foreach ($columns as $columnName => $definition) {
            if (isset($definition['primary'])) {
                $primaryKey = $columnName;
            }
            if (isset($definition['unique'])) {
                $uniqueKeys[] = $columnName;
            }
            if (isset($definition['index'])) {
                $indexes[] = $columnName;
            }
            if (isset($definition['foreign'])) {
                $foreignKeys[$columnName] = $definition['foreign'];
            }

            $columnDefinitions[] = $this->buildColumnDefinition($columnName, $definition);
        }

        $sql = "CREATE TABLE IF NOT EXISTS " . $this->escapeIdentifier($tableName) . " (\n";
        $sql .= implode(",\n", $columnDefinitions);

        // Add Primary Key
        if ($primaryKey) {
            $sql .= ",\nPRIMARY KEY (" . $this->escapeIdentifier($primaryKey) . ")";
        }

        // Add Unique Keys
        foreach ($uniqueKeys as $uniqueKey) {
            $sql .= ",\nUNIQUE KEY " . $this->escapeIdentifier("uk_" . $uniqueKey) . 
                   " (" . $this->escapeIdentifier($uniqueKey) . ")";
        }

        // Add Indexes
        foreach ($indexes as $index) {
            $sql .= ",\nINDEX " . $this->escapeIdentifier("idx_" . $index) . 
                   " (" . $this->escapeIdentifier($index) . ")";
        }

        // Add Foreign Keys
        foreach ($foreignKeys as $column => $definition) {
            $sql .= ",\nFOREIGN KEY (" . $this->escapeIdentifier($column) . ") " .
                   "REFERENCES " . $this->escapeIdentifier($definition['table']) . 
                   "(" . $this->escapeIdentifier($definition['column']) . ") " .
                   "ON DELETE " . ($definition['onDelete'] ?? 'RESTRICT') . " " .
                   "ON UPDATE " . ($definition['onUpdate'] ?? 'CASCADE');
        }

        $sql .= "\n) ENGINE=" . $engine . " DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci";

        return $this->execute($sql);
    }

    private function buildColumnDefinition($columnName, array $definition) {
        $type = strtoupper($definition['type']);
        $length = isset($definition['length']) ? "({$definition['length']})" : '';
        $nullable = isset($definition['nullable']) && $definition['nullable'] ? 'NULL' : 'NOT NULL';
        $default = isset($definition['default']) ? "DEFAULT " . 
                  (is_string($definition['default']) ? "'" . $definition['default'] . "'" : $definition['default']) : '';
        $autoIncrement = isset($definition['autoIncrement']) && $definition['autoIncrement'] ? 'AUTO_INCREMENT' : '';
        
        return trim($this->escapeIdentifier($columnName) . " {$type}{$length} {$nullable} {$default} {$autoIncrement}");
    }

    public function dropTable($tableName) {
        return $this->execute("DROP TABLE IF EXISTS " . $this->escapeIdentifier($tableName));
    }

    public function truncateTable($tableName) {
        return $this->execute("TRUNCATE TABLE " . $this->escapeIdentifier($tableName));
    }

    public function listTables() {
        return $this->query("SHOW TABLES")->fetchAll(PDO::FETCH_COLUMN);
    }

    public function getTableSchema($tableName) {
        return $this->query("DESCRIBE " . $this->escapeIdentifier($tableName))->fetchAll();
    }

    // Enhanced Data Manipulation Methods
    public function insertBatch($table, array $rows) {
        if (empty($rows)) {
            return 0;
        }

        $first = reset($rows);
        $fields = array_keys($first);
        $placeholders = '(' . str_repeat('?,', count($fields) - 1) . '?)';
        $allPlaceholders = str_repeat($placeholders . ',', count($rows) - 1) . $placeholders;

        $query = "INSERT INTO " . $this->escapeIdentifier($table) . 
                 " (" . implode(', ', array_map([$this, 'escapeIdentifier'], $fields)) . 
                 ") VALUES " . $allPlaceholders;

        $values = [];
        foreach ($rows as $row) {
            $values = array_merge($values, array_values($row));
        }

        return $this->execute($query, $values);
    }

    public function insertIgnore($table, array $data) {
        $fields = array_keys($data);
        $values = array_values($data);
        $placeholders = str_repeat('?,', count($fields) - 1) . '?';
        
        $query = "INSERT IGNORE INTO " . $this->escapeIdentifier($table) . 
                 " (" . implode(', ', array_map([$this, 'escapeIdentifier'], $fields)) . 
                 ") VALUES ($placeholders)";
        
        return $this->execute($query, $values);
    }

    public function replace($table, array $data) {
        $fields = array_keys($data);
        $values = array_values($data);
        $placeholders = str_repeat('?,', count($fields) - 1) . '?';
        
        $query = "REPLACE INTO " . $this->escapeIdentifier($table) . 
                 " (" . implode(', ', array_map([$this, 'escapeIdentifier'], $fields)) . 
                 ") VALUES ($placeholders)";
        
        return $this->execute($query, $values);
    }

    // Enhanced Select Methods
    public function selectDistinct($table, $columns = '*', $where = null, $params = [], $orderBy = null, $limit = null) {
        $query = "SELECT DISTINCT " . ($columns === '*' ? '*' : implode(', ', array_map([$this, 'escapeIdentifier'], (array)$columns)));
        $query .= " FROM " . $this->escapeIdentifier($table);
        
        if ($where) {
            $query .= " WHERE " . $where;
        }
        
        if ($orderBy) {
            $query .= " ORDER BY " . $orderBy;
        }
        
        if ($limit) {
            $query .= " LIMIT " . (int)$limit;
        }
        
        return $this->query($query, $params);
    }

    public function selectWithCount($table, $countColumn, $where = null, $params = [], $groupBy = null) {
        $query = "SELECT *, COUNT(" . $this->escapeIdentifier($countColumn) . ") as count";
        $query .= " FROM " . $this->escapeIdentifier($table);
        
        if ($where) {
            $query .= " WHERE " . $where;
        }
        
        if ($groupBy) {
            $query .= " GROUP BY " . $groupBy;
        }
        
        return $this->query($query, $params);
    }

    // Aggregation Methods
    public function count($table, $column = '*', $where = null, $params = []) {
        $query = "SELECT COUNT(" . ($column === '*' ? '*' : $this->escapeIdentifier($column)) . ") as count";
        $query .= " FROM " . $this->escapeIdentifier($table);
        
        if ($where) {
            $query .= " WHERE " . $where;
        }
        
        return $this->query($query, $params)->fetch()['count'];
    }

    public function sum($table, $column, $where = null, $params = []) {
        $query = "SELECT SUM(" . $this->escapeIdentifier($column) . ") as sum";
        $query .= " FROM " . $this->escapeIdentifier($table);
        
        if ($where) {
            $query .= " WHERE " . $where;
        }
        
        return $this->query($query, $params)->fetch()['sum'];
    }

    public function avg($table, $column, $where = null, $params = []) {
        $query = "SELECT AVG(" . $this->escapeIdentifier($column) . ") as avg";
        $query .= " FROM " . $this->escapeIdentifier($table);
        
        if ($where) {
            $query .= " WHERE " . $where;
        }
        
        return $this->query($query, $params)->fetch()['avg'];
    }

    // Schema Information Methods
    public function columnExists($table, $column) {
        try {
            $this->query("SELECT " . $this->escapeIdentifier($column) . 
                        " FROM " . $this->escapeIdentifier($table) . " LIMIT 1");
            return true;
        } catch (Exception $e) {
            return false;
        }
    }

    public function tableExists($table) {
        $query = "SHOW TABLES LIKE ?";
        return $this->query($query, [$table])->rowCount() > 0;
    }

    // Utility Methods
    public function ping() {
        try {
            $this->connection->query('SELECT 1');
            return true;
        } catch (PDOException $e) {
            return false;
        }
    }

    public function getLastQuery() {
        return $this->stmt ? $this->stmt->queryString : null;
    }

    public function quote($value) {
        return $this->connection->quote($value);
    }

    public static function getInstance($host = null, $username = null, $password = null, $database = null) {
        if (self::$instance === null) {
            self::$instance = new self($host, $username, $password, $database);
        }
        return self::$instance;
    }

    // Prevent cloning of the instance
    private function __clone() {}

    // Begin transaction with savepoint support
    public function beginTransaction() {
        if ($this->transactionCount === 0) {
            $this->connection->beginTransaction();
        } else {
            $this->connection->exec("SAVEPOINT LEVEL{$this->transactionCount}");
        }
        $this->transactionCount++;
        return $this;
    }

    // Commit transaction with savepoint support
    public function commit() {
        $this->transactionCount--;
        if ($this->transactionCount === 0) {
            $this->connection->commit();
        }
        return $this;
    }

    // Rollback transaction with savepoint support
    public function rollback() {
        if ($this->transactionCount === 0) {
            return $this;
        }
        $this->transactionCount--;
        if ($this->transactionCount === 0) {
            $this->connection->rollBack();
        } else {
            $this->connection->exec("ROLLBACK TO SAVEPOINT LEVEL{$this->transactionCount}");
        }
        return $this;
    }

    // Secure insert method
    public function insert($table, array $data) {
        $fields = array_keys($data);
        $values = array_values($data);
        $placeholders = str_repeat('?,', count($fields) - 1) . '?';
        
        $query = "INSERT INTO " . $this->escapeIdentifier($table) . 
                 " (" . implode(', ', array_map([$this, 'escapeIdentifier'], $fields)) . 
                 ") VALUES ($placeholders)";
        
        return $this->execute($query, $values);
    }

    // Secure select method with optional conditions
    public function select($table, $columns = '*', $where = null, $params = [], $orderBy = null, $limit = null) {
        $query = "SELECT " . ($columns === '*' ? '*' : implode(', ', array_map([$this, 'escapeIdentifier'], (array)$columns)));
        $query .= " FROM " . $this->escapeIdentifier($table);
        
        if ($where) {
            $query .= " WHERE " . $where;
        }
        
        if ($orderBy) {
            $query .= " ORDER BY " . $orderBy;
        }
        
        if ($limit) {
            $query .= " LIMIT " . (int)$limit;
        }
        
        return $this->query($query, $params);
    }

    // Random select method
    public function randomSelect($table, $columns = '*', $limit = 1) {
        return $this->select($table, $columns, null, [], "RAND()", $limit);
    }

    // Secure update method
    public function update($table, array $data, $where = null, $params = []) {
        $sets = [];
        $values = [];
        
        foreach ($data as $field => $value) {
            $sets[] = $this->escapeIdentifier($field) . " = ?";
            $values[] = $value;
        }
        
        $query = "UPDATE " . $this->escapeIdentifier($table) . 
                 " SET " . implode(', ', $sets);
        
        if ($where) {
            $query .= " WHERE " . $where;
            $values = array_merge($values, $params);
        }
        
        return $this->execute($query, $values);
    }

    // Join method
    public function join($table1, $table2, $joinType, $condition, $columns = '*', $where = null, $params = []) {
        $query = "SELECT " . ($columns === '*' ? '*' : implode(', ', array_map([$this, 'escapeIdentifier'], (array)$columns)));
        $query .= " FROM " . $this->escapeIdentifier($table1);
        $query .= " " . strtoupper($joinType) . " JOIN " . $this->escapeIdentifier($table2);
        $query .= " ON " . $condition;
        
        if ($where) {
            $query .= " WHERE " . $where;
        }
        
        return $this->query($query, $params);
    }

    // Custom query method
    public function query($query, $params = []) {
        try {
            $stmt = $this->connection->prepare($query);
            $stmt->execute($params);
            return $stmt;
        } catch (PDOException $e) {
            throw new Exception("Query failed: " . $e->getMessage());
        }
    }

    // Execute query and return number of affected rows
    private function execute($query, $params = []) {
        try {
            $stmt = $this->connection->prepare($query);
            $stmt->execute($params);
            return $stmt->rowCount();
        } catch (PDOException $e) {
            throw new Exception("Execute failed: " . $e->getMessage());
        }
    }

    // Escape identifier method to prevent SQL injection in table/column names
    private function escapeIdentifier($identifier) {
        return '`' . str_replace('`', '``', $identifier) . '`';
    }

    // Get last inserted ID
    public function lastInsertId() {
        return $this->connection->lastInsertId();
    }

    // Close connection
    public function __destruct() {
        $this->connection = null;
    }

}
