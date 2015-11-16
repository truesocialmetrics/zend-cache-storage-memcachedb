<?php
namespace TweeMemcacheDb\Cache\Storage\Adapter;

use SSDB as SsdbResource;
use Zend\Cache\Exception;
use Zend\Cache\Storage\Adapter\AdapterOptions;

class MemcacheDbOptions extends AdapterOptions
{
    const TYPE_MASTER = 'master';
    const TYPE_SLAVE  = 'slave';

    /**
     * A memcached master resource to share
     *
     * @var null|SsdbResource
     */
    protected $memcachedMasterResource;

    /**
     * A memcached slave resource to share
     *
     * @var null|SsdbResource
     */
    protected $memcachedSlaveResource;

    /**
     * List of memcached servers to add on initialize
     *
     * @var string
     */
    protected $servers = array(
        array(
            'host'   => '127.0.0.1',
            'port'   => 8888,
            'weight' => 0,
            'type'   => self::TYPE_MASTER,
        ),
        array(
            'host'   => '127.0.0.1',
            'port'   => 8888,
            'weight' => 0,
            'type'   => self::TYPE_SLAVE,
        ),
    );

    /**
     * List of Libmemcached options to set on initialize
     *
     * @var array
     */
    protected $libOptions = array();

    /**
     * Set namespace.
     *
     * The option Memcached::OPT_PREFIX_KEY will be used as the namespace.
     * It can't be longer than 128 characters.
     *
     * @see AdapterOptions::setNamespace()
     * @see MemcachedOptions::setPrefixKey()
     */
    public function setNamespace($namespace)
    {
        $namespace = (string) $namespace;

        if (128 < strlen($namespace)) {
            throw new Exception\InvalidArgumentException(sprintf(
                '%s expects a prefix key of no longer than 128 characters',
                __METHOD__
            ));
        }

        return parent::setNamespace($namespace);
    }

    /**
     * A memcached master resource to share
     *
     * @param null|SsdbResource $memcachedResource
     * @return MemcachedOptions
     */
    public function setMemcachedMasterResource(SsdbResource $memcachedResource = null)
    {
        if ($this->memcachedMasterResource !== $memcachedResource) {
            $this->triggerOptionEvent('memcached_resource', $memcachedResource);
            $this->memcachedMasterResource = $memcachedResource;
        }
        return $this;
    }

    /**
     * A memcached slave resource to share
     *
     * @param null|SsdbResource $memcachedResource
     * @return MemcachedOptions
     */
    public function setMemcachedSlaveResource(SsdbResource $memcachedResource = null)
    {
        if ($this->memcachedSlaveResource !== $memcachedResource) {
            $this->triggerOptionEvent('memcached_resource', $memcachedResource);
            $this->memcachedSlaveResource = $memcachedResource;
        }
        return $this;
    }

    /**
     * Get memcached master resource to share
     *
     * @return null|SsdbResource
     */
    public function getMemcachedMasterResource()
    {
        return $this->memcachedMasterResource;
    }

    /**
     * Get memcached slave resource to share
     *
     * @return null|SsdbResource
     */
    public function getMemcachedSlaveResource()
    {
        return $this->memcachedSlaveResource;
    }

    /**
     * Add a server to the list
     *
     * @param  string $host
     * @param  int $port
     * @param  int $weight
     * @return MemcachedOptions
     */
    public function addServer($host, $port = 8888, $weight = 0, $type = self::TYPE_SLAVE)
    {
        $new = array(
            'host'   => $host,
            'port'   => $port,
            'weight' => $weight,
            'type'   => $type,
        );

        foreach ($this->servers as $server) {
            $diff = array_diff($new, $server);
            if (empty($diff)) {
                // Done -- server is already present
                return $this;
            }
        }

        $this->servers[] = $new;
        return $this;
    }

    /**
     * Set a list of memcached servers to add on initialize
     *
     * @param string|array $servers list of servers
     * @return MemcachedOptions
     * @throws Exception\InvalidArgumentException
     */
    public function setServers($servers)
    {
        if (!is_array($servers)) {
            return $this->setServers(explode(',', $servers));
        }

        $this->servers = array();
        foreach ($servers as $server) {
            // default values
            $host   = null;
            $port   = 8888;
            $weight = 1;
            $type   = self::TYPE_SLAVE;

            if (!is_array($server) && !is_string($server)) {
                throw new Exception\InvalidArgumentException('Invalid server specification provided; must be an array or string');
            }

            // parse a single server from an array
            if (is_array($server)) {
                if (!isset($server[0]) && !isset($server['host'])) {
                    throw new Exception\InvalidArgumentException("Invalid list of servers given");
                }

                // array(array(<host>[, <port>[, <weight>]])[, ...])
                if (isset($server[0])) {
                    $host   = (string) $server[0];
                    $port   = isset($server[1]) ? (int) $server[1] : $port;
                    $weight = isset($server[2]) ? (int) $server[2] : $weight;
                    $type   = isset($server[3]) ? $server[3] : $type;
                }

                // array(array('host' => <host>[, 'port' => <port>[, 'weight' => <weight>]])[, ...])
                if (!isset($server[0]) && isset($server['host'])) {
                    $host   = (string)$server['host'];
                    $port   = isset($server['port'])   ? (int) $server['port']   : $port;
                    $weight = isset($server['weight']) ? (int) $server['weight'] : $weight;
                    $type   = isset($server['type']) ? $server['type'] : $type;
                }
            }

            // parse a single server from a string
            if (!is_array($server)) {
                $server = trim($server);
                if (strpos($server, '://') === false) {
                    $server = 'tcp://' . $server;
                }

                $server = parse_url($server);
                if (!$server) {
                    throw new Exception\InvalidArgumentException("Invalid list of servers given");
                }

                $host = $server['host'];
                $port = isset($server['port']) ? (int)$server['port'] : $port;

                if (isset($server['query'])) {
                    $query = null;
                    parse_str($server['query'], $query);
                    if (isset($query['weight'])) {
                        $weight = (int)$query['weight'];
                    }
                    if (isset($query['type'])) {
                        $type = (string)$query['type'];
                    }
                }
            }

            if (!$host) {
                throw new Exception\InvalidArgumentException('The list of servers must contain a host value.');
            }

            $this->addServer($host, $port, $weight, $type);
        }

        if (!count($this->getMasterServers())) {
            throw new Exception\InvalidArgumentException('No master found in provided definition');
        }
        return $this;
    }

    /**
     * Get Servers
     *
     * @return array
     */
    public function getServers()
    {
        return $this->servers;
    }

    /**
     * Get Master Servers
     *
     * @return array
     */
    public function getMasterServers()
    {
        $type = self::TYPE_MASTER; // php 5.3 hack
        return array_values(array_filter($this->servers, function($server) use ($type) {
            return $server['type'] == $type;
        }));
    }

    /**
     * Get Slave Servers
     *
     * @return array
     */
    public function getSlaveServers()
    {
        return $this->getServers();
    }

    /**
     * Set libmemcached options
     *
     * @param array $libOptions
     * @return MemcachedOptions
     * @link http://php.net/manual/memcached.constants.php
     */
    public function setLibOptions(array $libOptions)
    {
        $normalizedOptions = array();
        foreach ($libOptions as $key => $value) {
            $this->normalizeLibOptionKey($key);
            $normalizedOptions[$key] = $value;
        }

        $this->triggerOptionEvent('lib_options', $normalizedOptions);
        $this->libOptions = array_diff_key($this->libOptions, $normalizedOptions) + $normalizedOptions;

        return $this;
    }

    /**
     * Set libmemcached option
     *
     * @param string|int $key
     * @param mixed      $value
     * @return MemcachedOptions
     * @link http://php.net/manual/memcached.constants.php
     */
    public function setLibOption($key, $value)
    {
        $this->normalizeLibOptionKey($key);
        $this->triggerOptionEvent('lib_options', array($key, $value));
        $this->libOptions[$key] = $value;

        return $this;
    }

    /**
     * Get libmemcached options
     *
     * @return array
     * @link http://php.net/manual/memcached.constants.php
     */
    public function getLibOptions()
    {
        return $this->libOptions;
    }

    /**
     * Get libmemcached option
     *
     * @param string|int $key
     * @return mixed
     * @link http://php.net/manual/memcached.constants.php
     */
    public function getLibOption($key)
    {
        $this->normalizeLibOptionKey($key);
        if (isset($this->libOptions[$key])) {
            return $this->libOptions[$key];
        }
        return null;
    }

    /**
     * Normalize libmemcached option name into it's constant value
     *
     * @param string|int $key
     * @throws Exception\InvalidArgumentException
     */
    protected function normalizeLibOptionKey(& $key)
    {
        if (is_string($key)) {
            $const = 'Memcached::OPT_' . str_replace(array(' ', '-'), '_', strtoupper($key));
            if (!defined($const)) {
                throw new Exception\InvalidArgumentException("Unknown libmemcached option '{$key}' ({$const})");
            }
            $key = constant($const);
        } else {
            $key = (int) $key;
        }
    }
}
