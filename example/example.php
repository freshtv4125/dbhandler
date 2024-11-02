<?php
require_once("../src/DbHandler.php");
try {
            // Initialize database handler
            $db = DbHandler::getInstance('localhost', 'root', '');
            
            // Create new database
            $db->createDatabase('my_application');
            
            // Create users table
            $db->createTable('users', [
                'id' => [
                    'type' => 'int',
                    'length' => 11,
                    'primary' => true,
                    'autoIncrement' => true
                ],
                'username' => [
                    'type' => 'varchar',
                    'length' => 255,
                    'unique' => true,
                    'nullable' => false
                ],
                'email' => [
                    'type' => 'varchar',
                    'length' => 255,
                    'unique' => true,
                    'nullable' => false
                ],
                'status' => [
                    'type' => 'enum',
                    'length' => "'active','inactive','banned'",
                    'default' => 'active'
                ],
                'created_at' => [
                    'type' => 'timestamp',
                    'default' => 'CURRENT_TIMESTAMP'
                ]
            ]);

            // Batch insert example
            $users = [
                ['username' => 'user1', 'email' => 'user1@example.com'],
                ['username' => 'user2', 'email' => 'user2@example.com'],
                ['username' => 'user3', 'email' => 'user3@example.com']
            ];
            $db->insertBatch('users', $users);

            // Aggregate functions example
            $totalUsers = $db->count('users');
            $activeUsers = $db->count('users', '*', 'status = ?', ['active']);
            
            // Select with count example
            $usersByStatus = $db->selectWithCount('users', 'id', null, [], 'status')
                               ->fetchAll();

            // Schema information
            $tables = $db->listTables();
            $userSchema = $db->getTableSchema('users');

            echo "Database operations completed successfully!";
            
        } catch (Exception $e) {
            echo "Error: " . $e->getMessage();
        }
