<?php
namespace TweeMemcacheDb\Cache\Storage\Adapter;
use PHPUnit_Framework_TestCase;

class MemcacheDbOptionsTest extends PHPUnit_Framework_TestCase
{
    public function testAddServer()
    {
        $options = new MemcacheDbOptions;
        $options->addServer('localhost');
        $this->assertAttributeEquals(
            array(
                array('host' => '127.0.0.1', 'port' => 8888, 'weight' => 0, 'type' => MemcacheDbOptions::TYPE_MASTER),
                array('host' => '127.0.0.1', 'port' => 8888, 'weight' => 0, 'type' => MemcacheDbOptions::TYPE_SLAVE),
                array('host' => 'localhost', 'port' => 8888, 'weight' => 0, 'type' => MemcacheDbOptions::TYPE_SLAVE),
            ),
            'servers',
            $options
        );
    }

    public function testSetServers()
    {
        $options = new MemcacheDbOptions;
        $options->setServers('localhost:8888?weight=1&type=master');
        $this->assertAttributeEquals(
            array(
                array('host' => 'localhost', 'port' => 8888, 'weight' => 1, 'type' => MemcacheDbOptions::TYPE_MASTER),
            ),
            'servers',
            $options
        );
    }

    /**
     *
     * @expectedException Zend\Cache\Exception\InvalidArgumentException
     */
    public function testSetServersNoMaster()
    {
        $options = new MemcacheDbOptions;
        $options->setServers('localhost');
        $this->assertAttributeEquals(
            array(
                array('host' => 'localhost', 'port' => 8888, 'weight' => 1, 'type' => MemcacheDbOptions::TYPE_SLAVE),
            ),
            'servers',
            $options
        );
    }

    public function testGetMasters()
    {
        $options = new MemcacheDbOptions;
        $options->addServer('localhost');
        $this->assertEquals(array(
            array('host' => '127.0.0.1', 'port' => 8888, 'weight' => 0, 'type' => MemcacheDbOptions::TYPE_MASTER)
        ), $options->getMasterServers());
    }

    public function testGetSlaves()
    {
        $options = new MemcacheDbOptions;
        $options->addServer('localhost');
        $this->assertEquals(array(
            array('host' => '127.0.0.1', 'port' => 8888, 'weight' => 0, 'type' => MemcacheDbOptions::TYPE_SLAVE),
            array('host' => 'localhost', 'port' => 8888, 'weight' => 0, 'type' => MemcacheDbOptions::TYPE_SLAVE),
        ), $options->getSlaveServers());
    }
}
