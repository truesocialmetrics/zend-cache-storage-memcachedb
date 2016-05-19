<?php
namespace TweeMemcacheDb\Cache\Storage\Adapter;

use SSDB as SsdbResource;
use stdClass;
use Traversable;
use Zend\Cache\Exception;
use Zend\Cache\Storage\AvailableSpaceCapableInterface;
use Zend\Cache\Storage\Capabilities;
use Zend\Cache\Storage\FlushableInterface;
use Zend\Cache\Storage\TotalSpaceCapableInterface;
use Zend\Cache\Storage\Adapter\AbstractAdapter;

class MemcacheDb extends AbstractAdapter implements
    AvailableSpaceCapableInterface,
    FlushableInterface,
    TotalSpaceCapableInterface
{
    /**
     * Major version of ext/memcached
     *
     * @var null|int
     */
    protected static $extMemcachedMajorVersion;

    /**
     * The memcached master resource
     *
     * @var SsdbResource
     */
    protected $memcachedMasterResource;

    /**
     * The memcached slave resource
     *
     * @var SsdbResource
     */
    protected $memcachedSlaveResource;

    /**
     * Constructor
     *
     * @param  null|array|Traversable|MemcacheDbOptions $options
     * @throws Exception\ExceptionInterface
     */
    public function __construct($options = null)
    {
        if (static::$extMemcachedMajorVersion === null) {
            $v = (string) phpversion('memcached');
            static::$extMemcachedMajorVersion = ($v !== '') ? (int)$v[0] : 0;
        }

        if (static::$extMemcachedMajorVersion < 1) {
            throw new Exception\ExtensionNotLoadedException('Need ext/memcached version >= 1.0.0');
        }

        parent::__construct($options);
    }

    /**
     * Initialize the internal memcached master resource
     *
     * @return SsdbResource
     */
    protected function getMemcachedMasterResource()
    {
        if ($this->memcachedMasterResource) {
            return $this->memcachedMasterResource;
        }

        $options = $this->getOptions();

        // use a configured resource or a new one
        $memcached = $options->getMemcachedMasterResource() ?: new SsdbResource();

        // init servers
        $servers = $options->getMasterServers();
        shuffle($servers);
        $server = reset($servers);
        $memcached->connect($server['host'], $server['port']);
        //$memcached->option(SsdbResource::OPT_SERIALIZER, SsdbResource::SERIALIZER_PHP);

        // use the initialized resource
        $this->memcachedMasterResource = $memcached;

        return $this->memcachedMasterResource;
    }

    /**
     * Initialize the internal memcached slave resource
     *
     * @return SsdbResource
     */
    protected function getMemcachedSlaveResource()
    {
        if ($this->memcachedSlaveResource) {
            return $this->memcachedSlaveResource;
        }

        $options = $this->getOptions();

        // use a configured resource or a new one
        $memcached = $options->getMemcachedSlaveResource() ?: new SsdbResource();

        // init servers
        $servers = $options->getSlaveServers();
        shuffle($servers);
        $server = reset($servers);
        $memcached->connect($server['host'], $server['port']);
        $memcached->option(SsdbResource::OPT_SERIALIZER, SsdbResource::SERIALIZER_PHP);

        // use the initialized resource
        $this->memcachedSlaveResource = $memcached;

        return $this->memcachedSlaveResource;
    }

    /* options */

    /**
     * Set options.
     *
     * @param  array|Traversable|MemcacheDbOptions $options
     * @return Memcached
     * @see    getOptions()
     */
    public function setOptions($options)
    {
        if (!$options instanceof MemcacheDbOptions) {
            $options = new MemcacheDbOptions($options);
        }

        return parent::setOptions($options);
    }

    /**
     * Get options.
     *
     * @return MemcacheDbOptions
     * @see setOptions()
     */
    public function getOptions()
    {
        if (!$this->options) {
            $this->setOptions(new MemcacheDbOptions());
        }
        return $this->options;
    }

    /* FlushableInterface */

    /**
     * Flush the whole storage
     *
     * @return boolean
     */
    public function flush()
    {
        return true;
    }

    /* TotalSpaceCapableInterface */

    /**
     * Get total space in bytes
     *
     * @return int|float
     */
    public function getTotalSpace()
    {
        $memc  = $this->getMemcachedMasterResource();
        return $memc->getStats();
        if ($stats === false) {
            throw new Exception\RuntimeException($memc->getResultMessage());
        }

        $mem = array_pop($stats);
        return $mem['limit_maxbytes'];
    }

    /* AvailableSpaceCapableInterface */

    /**
     * Get available space in bytes
     *
     * @return int|float
     */
    public function getAvailableSpace()
    {
        $memc  = $this->getMemcachedMasterResource();
        $stats = $memc->getStats();
        if ($stats === false) {
            throw new Exception\RuntimeException($memc->getResultMessage());
        }

        $mem = array_pop($stats);
        return $mem['limit_maxbytes'] - $mem['bytes'];
    }

    /* reading */

    /**
     * Internal method to get an item.
     *
     * @param  string  $normalizedKey
     * @param  boolean $success
     * @param  mixed   $casToken
     * @return mixed Data on success, null on failure
     * @throws Exception\ExceptionInterface
     */
    protected function internalGetItem(& $normalizedKey, & $success = null, & $casToken = null)
    {
        $memc = $this->getMemcachedSlaveResource();
        return $memc->get($normalizedKey);
    }

    /**
     * Internal method to get multiple items.
     *
     * @param  array $normalizedKeys
     * @return array Associative array of keys and values
     * @throws Exception\ExceptionInterface
     */
    protected function internalGetItems(array & $normalizedKeys)
    {
        $memc   = $this->getMemcachedSlaveResource();
        return array_map(function($value) {
            if (!is_string($value)) return $value;
            return unserialize($value);
        }, $memc->multi_get($normalizedKeys));
    }

    /**
     * Internal method to test if an item exists.
     *
     * @param  string $normalizedKey
     * @return boolean
     * @throws Exception\ExceptionInterface
     */
    protected function internalHasItem(& $normalizedKey)
    {
        $memc  = $this->getMemcachedSlaveResource();
        return $memc->exists($normalizedKey);
    }

    /**
     * Internal method to test multiple items.
     *
     * @param  array $normalizedKeys
     * @return array Array of found keys
     * @throws Exception\ExceptionInterface
     */
    protected function internalHasItems(array & $normalizedKeys)
    {
        $memc   = $this->getMemcachedSlaveResource();
        $map = array();
        foreach ($normalizedKeys as $key) {
            $map[$key] = $memc->exists($key);
        }
        return $map;
    }

    /**
     * Get metadata of multiple items
     *
     * @param  array $normalizedKeys
     * @return array Associative array of keys and metadata
     * @throws Exception\ExceptionInterface
     */
    protected function internalGetMetadatas(array & $normalizedKeys)
    {
        $memc   = $this->getMemcachedSlaveResource();
        return array_map(function($value) {
            if (!is_string($value)) return $value;
            return unserialize($value);
        }, $memc->multi_get($normalizedKeys));
    }

    /* writing */

    /**
     * Internal method to store an item.
     *
     * @param  string $normalizedKey
     * @param  mixed  $value
     * @return boolean
     * @throws Exception\ExceptionInterface
     */
    protected function internalSetItem(& $normalizedKey, & $value)
    {
        $memc = $this->getMemcachedMasterResource();
        return $memc->set($normalizedKey, serialize($value));
    }

    /**
     * Internal method to store multiple items.
     *
     * @param  array $normalizedKeyValuePairs
     * @return array Array of not stored keys
     * @throws Exception\ExceptionInterface
     */
    protected function internalSetItems(array & $normalizedKeyValuePairs)
    {
        $memc = $this->getMemcachedMasterResource();
        return $memc->multi_set(array_map('serialize', $normalizedKeyValuePairs));
    }

    /**
     * Add an item.
     *
     * @param  string $normalizedKey
     * @param  mixed  $value
     * @return boolean
     * @throws Exception\ExceptionInterface
     */
    protected function internalAddItem(& $normalizedKey, & $value)
    {
        $memc = $this->getMemcachedMasterResource();
        return $memc->incr($normalizedKey, $value);
    }

    /**
     * Internal method to replace an existing item.
     *
     * @param  string $normalizedKey
     * @param  mixed  $value
     * @return boolean
     * @throws Exception\ExceptionInterface
     */
    protected function internalReplaceItem(& $normalizedKey, & $value)
    {
        $memc = $this->getMemcachedMasterResource();
        return $memc->getset($normalizedKey, serialize($value));
    }

    /**
     * Internal method to set an item only if token matches
     *
     * @param  mixed  $token
     * @param  string $normalizedKey
     * @param  mixed  $value
     * @return boolean
     * @throws Exception\ExceptionInterface
     * @see    getItem()
     * @see    setItem()
     */
    protected function internalCheckAndSetItem(& $token, & $normalizedKey, & $value)
    {
        $memc       = $this->getMemcachedMasterResource();
        return $memc->getset($token, $normalizedKey, serialize($value));
    }

    /**
     * Internal method to remove an item.
     *
     * @param  string $normalizedKey
     * @return boolean
     * @throws Exception\ExceptionInterface
     */
    protected function internalRemoveItem(& $normalizedKey)
    {
        $memc = $this->getMemcachedMasterResource();
        return $memc->del($normalizedKey);
    }

    /**
     * Internal method to remove multiple items.
     *
     * @param  array $normalizedKeys
     * @return array Array of not removed keys
     * @throws Exception\ExceptionInterface
     */
    protected function internalRemoveItems(array & $normalizedKeys)
    {
        $memc = $this->getMemcachedMasterResource();
        return $memc->multi_del($normalizedKeys);
    }

    /**
     * Internal method to increment an item.
     *
     * @param  string $normalizedKey
     * @param  int    $value
     * @return int|boolean The new value on success, false on failure
     * @throws Exception\ExceptionInterface
     */
    protected function internalIncrementItem(& $normalizedKey, & $value)
    {
        $memc     = $this->getMemcachedMasterResource();
        $value    = (int) $value;
        return $memc->incr($normalizedKey, $value);
    }

    /**
     * Internal method to decrement an item.
     *
     * @param  string $normalizedKey
     * @param  int    $value
     * @return int|boolean The new value on success, false on failure
     * @throws Exception\ExceptionInterface
     */
    protected function internalDecrementItem(& $normalizedKey, & $value)
    {
        $memc     = $this->getMemcachedMasterResource();
        $value    = (int)$value * -1;
        return $memc->incr($normalizedKey, $value);
    }

    /* status */

    /**
     * Internal method to get capabilities of this adapter
     *
     * @return Capabilities
     */
    protected function internalGetCapabilities()
    {
        if ($this->capabilities === null) {
            $this->capabilityMarker = new stdClass();
            $this->capabilities     = new Capabilities(
                $this,
                $this->capabilityMarker,
                array(
                    'supportedDatatypes' => array(
                        'NULL'     => true,
                        'boolean'  => true,
                        'integer'  => true,
                        'double'   => true,
                        'string'   => true,
                        'array'    => true,
                        'object'   => 'object',
                        'resource' => false,
                    ),
                    'supportedMetadata'  => array(),
                    'minTtl'             => 1,
                    'maxTtl'             => 0,
                    'staticTtl'          => true,
                    'ttlPrecision'       => 1,
                    'useRequestTime'     => false,
                    'expiredRead'        => false,
                    'maxKeyLength'       => 255,
                    'namespaceIsPrefix'  => true,
                )
            );
        }

        return $this->capabilities;
    }
}