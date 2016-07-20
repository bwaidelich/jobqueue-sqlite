<?php
namespace Flownative\JobQueue\Sqlite\Queue;

/*
 * This file is part of the Flownative.JobQueue.Sqlite package.
 *
 * (c) Contributors to the package
 *
 * This package is Open Source Software. For the full copyright and license
 * information, please view the LICENSE file which was distributed with this
 * source code.
 */

use Flowpack\JobQueue\Common\Exception as JobQueueException;
use Flowpack\JobQueue\Common\Queue\Message;
use Flowpack\JobQueue\Common\Queue\QueueInterface;
use TYPO3\Flow\Annotations as Flow;
use TYPO3\Flow\Utility\Files;

/**
 * A queue implementation using Sqlite as the queue backend
 */
class SqliteQueue implements QueueInterface
{
    /**
     * @var string
     */
    protected $name;

    /**
     * @var string
     */
    protected $storageFolder;

    /**
     * @var \SQLite3
     */
    protected $connection;

    /**
     * @var integer
     */
    protected $defaultTimeout = 60;

    /**
     * @param string $name
     * @param array $options
     * @throws JobQueueException
     */
    public function __construct($name, array $options = [])
    {
        $this->name = $name;

        if (isset($options['defaultTimeout'])) {
            $this->defaultTimeout = $options['defaultTimeout'];
        }

        if (!isset($options['storageFolder'])) {
            throw new JobQueueException('No storageFolder configured for SqliteQueue.', 1445527553);
        }
        $this->storageFolder = $options['storageFolder'];
    }

    /**
     * Lifecycle method
     *
     * @return void
     */
    public function initializeObject()
    {
        $databaseFilePath = Files::concatenatePaths([$this->storageFolder, md5($this->name) . '.db']);
        $createDatabaseTables = false;
        if (!is_file($databaseFilePath)) {
            if (!is_dir($this->storageFolder)) {
                Files::createDirectoryRecursively($this->storageFolder);
            }
            $createDatabaseTables = true;
        }
        $this->connection = new \SQLite3($databaseFilePath);
        if ($createDatabaseTables) {
            $this->createQueueTable();
        }
    }

    /**
     * @inheritdoc
     */
    public function submit($payload, array $options = [])
    {
        $preparedStatement = $this->connection->prepare('INSERT INTO queue (payload, state) VALUES (:payload, :state);');
        $preparedStatement->bindValue(':payload', json_encode($payload));
        $preparedStatement->bindValue(':state', 'ready');
        $success = $preparedStatement->execute();
        if ($success === false) {
            return null;
        }
        return (string)$this->connection->lastInsertRowID();
    }

    /**
     * Wait for a message in the queue and return the message for processing
     *
     * @param int $timeout
     * @return Message The received message or NULL if a timeout occurred
     */
    public function waitAndTake($timeout = null)
    {
        $message = $this->reserveMessage($timeout);
        if ($message === null) {
            return null;
        }
        $this->connection->exec('DELETE FROM queue WHERE id = ' . (integer)$message->getIdentifier());
        return $message;
    }

    /**
     * Wait for a message in the queue and save the message to a safety queue
     *
     * @param int $timeout
     * @return Message
     */
    public function waitAndReserve($timeout = null)
    {
        return $this->reserveMessage($timeout);
    }

    /**
     * @param integer $timeout
     * @return Message
     */
    protected function reserveMessage($timeout = null)
    {
        if ($timeout === null) {
            $timeout = $this->defaultTimeout;
        }
        $startTime = time();
        do {
            $row = @$this->connection->querySingle('SELECT id, payload FROM queue WHERE state = "submitted" ORDER BY id ASC LIMIT 1', true);
            if (time() - $startTime >= $timeout) {
                return null;
            }
            if ($row !== []) {
                $this->connection->exec('UPDATE queue SET state = "reserved" WHERE id = ' . (integer)$row['id'] . ' AND state = "submitted"');
                if ($this->connection->changes() === 1) {
                    return $this->getMessageFromRow($row);
                }
            }
            sleep(1);
        } while (true);
    }

    /**
     * @inheritdoc
     */
    public function finish($messageId)
    {
        $this->connection->exec('DELETE FROM queue WHERE id=' . (int)$messageId);
        return $this->connection->changes() === 1;
    }

    /**
     * Peek for messages
     *
     * @param integer $limit
     * @return array Messages or empty array if no messages were present
     */
    public function peek($limit = 1)
    {
        $result = $this->connection->query('SELECT * FROM queue WHERE state = "submitted" ORDER BY id ASC LIMIT ' . (int)$limit);
        $messages = [];
        while ($resultRow = $result->fetchArray(SQLITE3_ASSOC)) {
            $messages[] = $this->getMessageFromRow($resultRow);
        }
        return $messages;
    }

    /**
     * @inheritdoc
     */
    public function count()
    {
        return (integer)$this->connection->querySingle('SELECT COUNT(id) FROM queue WHERE state = "submitted"');
    }

    /**
     * @return string
     */
    public function getName()
    {
        return $this->name;
    }

    /**
     * Puts a reserved message back to the queue
     *
     * @param string $messageId
     * @param array $options Simple key/value array with options that can be interpreted by the concrete implementation (optional)
     * @return void
     */
    public function release($messageId, array $options = [])
    {
        // TODO: Implement release() method.
    }

    /**
     * Removes a message from the active queue and marks it failed (bury)
     *
     * @param string $messageId
     * @return void
     */
    public function abort($messageId)
    {
        // TODO: Implement abort() method.
    }

    /**
     * @return void
     */
    protected function createQueueTable()
    {
        $this->connection->exec('CREATE TABLE queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            payload TEXT,
            state TEXT,
            failures INTEGER DEFAULT 0
        );');
    }


    /**
     * @param array $row
     * @return Message
     */
    protected function getMessageFromRow(array $row)
    {
        return new Message($row['id'], json_decode($row['payload'], true), 0);
    }

    /**
     * @return void
     */
    public function setUp()
    {
        // TODO: Implement setUp() method.
    }

    /**
     * Removes all messages from this queue
     *
     * Danger, all queued items will be lost!
     * This is a method primarily used in testing, not part of the API.
     *
     * @return void
     */
    public function flush()
    {
        $databaseFilePath = Files::concatenatePaths([$this->storageFolder, md5($this->name) . '.db']);
        Files::unlink($databaseFilePath);
    }
}
