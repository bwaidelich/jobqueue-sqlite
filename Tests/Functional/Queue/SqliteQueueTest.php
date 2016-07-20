<?php
namespace Flownative\JobQueue\Sqlite\Tests\Functional\Queue;

/*
 * This file is part of the Flownative.Jobqueue.Sqlite package.
 *
 * (c) Contributors to the package
 *
 * This package is Open Source Software. For the full copyright and license
 * information, please view the LICENSE file which was distributed with this
 * source code.
 */

use Flownative\JobQueue\Sqlite\Queue\SqliteQueue;
use Flowpack\JobQueue\Common\Tests\Functional\AbstractQueueTest;

/**
 * Functional test for SqliteQueue
 */
class SqliteQueueTest extends AbstractQueueTest
{

    /**
     * @inheritdoc
     */
    protected function getQueue()
    {
        return new SqliteQueue('Test-queue', $this->queueSettings);
    }

}