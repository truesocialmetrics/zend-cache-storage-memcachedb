<?php
namespace TweeMemcacheDb\Cache\Storage\Adapter;

use Memcached as MemcachedResource;
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
     * The memcached resource
     *
     * @var MemcachedResource
     */
    protected $memcachedResource;

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
     * Initialize the internal memcached resource
     *
     * @return MemcachedResource
     */
    protected function getMemcachedResource()
    {
        if ($this->memcachedResource) {
            return $this->memcachedResource;
        }

        $options = $this->getOptions();

        // use a configured resource or a new one
        $memcached = $options->getMemcachedResource() ?: new MemcachedResource();

        // init lib options
        if (static::$extMemcachedMajorVersion > 1) {
            $memcached->setOptions($options->getLibOptions());
        } else {
            foreach ($options->getLibOptions() as $k => $v) {
                $memcached->setOption($k, $v);
            }
        }
        $memcached->setOption(MemcachedResource::OPT_PREFIX_KEY, $options->getNamespace());

        // Allow updating namespace
        $this->getEventManager()->attach('option', function ($event) use ($memcached) {
            $params = $event->getParams();
            if (!isset($params['namespace'])) {
                // Cannot set lib options after initialization
                return;
            }
            $memcached->setOption(MemcachedResource::OPT_PREFIX_KEY, $params['namespace']);
        });

        // init servers
        $servers = $options->getServers();
        if ($servers) {
            $memcached->addServers($servers);
        }

        // use the initialized resource
        $this->memcachedResource = $memcached;

        return $this->memcachedResource;
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
        $memc = $this->getMemcachedResource();
        if (!$memc->flush()) {
            throw $this->getExceptionByResultCode($memc->getResultCode());
        }
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
        $memc  = $this->getMemcachedResource();
        $stats = $memc->getStats();
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
        $memc  = $this->getMemcachedResource();
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
        $memc = $this->getMemcachedResource();

        if (func_num_args() > 2) {
            $result = $memc->get($normalizedKey, null, $casToken);
        } else {
            $result = $memc->get($normalizedKey);
        }

        $success = true;
        if ($result === false || $result === null) {
            $rsCode = $memc->getResultCode();
            if ($rsCode == MemcachedResource::RES_NOTFOUND) {
                $result = null;
                $success = false;
            } elseif ($rsCode) {
                $success = false;
                throw $this->getExceptionByResultCode($rsCode);
            }
        }

        return $result;
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
        $memc   = $this->getMemcachedResource();
        $result = $memc->getMulti($normalizedKeys);
        if ($result === false) {
            throw $this->getExceptionByResultCode($memc->getResultCode());
        }

        return $result;
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
        $memc  = $this->getMemcachedResource();
        $value = $memc->get($normalizedKey);
        if ($value === false || $value === null) {
            $rsCode = $memc->getResultCode();
            if ($rsCode == MemcachedResource::RES_SUCCESS) {
                return true;
            } elseif ($rsCode == MemcachedResource::RES_NOTFOUND) {
                return false;
            } else {
                throw $this->getExceptionByResultCode($rsCode);
            }
        }

        return true;
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
        $memc   = $this->getMemcachedResource();
        $result = $memc->getMulti($normalizedKeys);
        if ($result === false) {
            throw $this->getExceptionByResultCode($memc->getResultCode());
        }

        return array_keys($result);
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
        $memc   = $this->getMemcachedResource();
        $result = $memc->getMulti($normalizedKeys);
        if ($result === false) {
            throw $this->getExceptionByResultCode($memc->getResultCode());
        }

        foreach ($result as & $value) {
            $value = array();
        }

        return $result;
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
        $memc       = $this->getMemcachedResource();
        if (!$memc->set($normalizedKey, $value)) {
            throw $this->getExceptionByResultCode($memc->getResultCode());
        }

        return true;
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
        $memc       = $this->getMemcachedResource();
        if (!$memc->setMulti($normalizedKeyValuePairs)) {
            throw $this->getExceptionByResultCode($memc->getResultCode());
        }

        return array();
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
        $memc       = $this->getMemcachedResource();
        if (!$memc->add($normalizedKey, $value)) {
            if ($memc->getResultCode() == MemcachedResource::RES_NOTSTORED) {
                return false;
            }
            throw $this->getExceptionByResultCode($memc->getResultCode());
        }

        return true;
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
        $memc       = $this->getMemcachedResource();
        if (!$memc->replace($normalizedKey, $value)) {
            $rsCode = $memc->getResultCode();
            if ($rsCode == MemcachedResource::RES_NOTSTORED) {
                return false;
            }
            throw $this->getExceptionByResultCode($rsCode);
        }

        return true;
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
        $memc       = $this->getMemcachedResource();
        $result     = $memc->cas($token, $normalizedKey, $value);

        if ($result === false) {
            $rsCode = $memc->getResultCode();
            if ($rsCode !== 0 && $rsCode != MemcachedResource::RES_DATA_EXISTS) {
                throw $this->getExceptionByResultCode($rsCode);
            }
        }


        return $result;
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
        $memc   = $this->getMemcachedResource();
        $result = $memc->delete($normalizedKey);

        if ($result === false) {
            $rsCode = $memc->getResultCode();
            if ($rsCode == MemcachedResource::RES_NOTFOUND) {
                return false;
            } elseif ($rsCode != MemcachedResource::RES_SUCCESS) {
                throw $this->getExceptionByResultCode($rsCode);
            }
        }

        return true;
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
        // support for removing multiple items at once has been added in ext/memcached-2.0.0
        if (static::$extMemcachedMajorVersion < 2) {
            return parent::internalRemoveItems($normalizedKeys);
        }

        $memc    = $this->getMemcachedResource();
        $rsCodes = $memc->deleteMulti($normalizedKeys);

        $missingKeys = array();
        foreach ($rsCodes as $key => $rsCode) {
            if ($rsCode !== true && $rsCode != MemcachedResource::RES_SUCCESS) {
                if ($rsCode != MemcachedResource::RES_NOTFOUND) {
                    throw $this->getExceptionByResultCode($rsCode);
                }
                $missingKeys[] = $key;
            }
        }

        return $missingKeys;
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
        $memc     = $this->getMemcachedResource();
        $value    = (int) $value;
        $newValue = $memc->increment($normalizedKey, $value);

        if ($newValue === false) {
            $rsCode = $memc->getResultCode();

            // initial value
            if ($rsCode == MemcachedResource::RES_NOTFOUND) {
                $newValue = $value;
                $memc->add($normalizedKey, $newValue);
                $rsCode = $memc->getResultCode();
            }

            if ($rsCode) {
                throw $this->getExceptionByResultCode($rsCode);
            }
        }

        return $newValue;
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
        $memc     = $this->getMemcachedResource();
        $value    = (int)$value;
        $newValue = $memc->decrement($normalizedKey, $value);

        if ($newValue === false) {
            $rsCode = $memc->getResultCode();

            // initial value
            if ($rsCode == MemcachedResource::RES_NOTFOUND) {
                $newValue = -$value;
                $memc->add($normalizedKey, $newValue);
                $rsCode = $memc->getResultCode();
            }

            if ($rsCode) {
                throw $this->getExceptionByResultCode($rsCode);
            }
        }

        return $newValue;
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

    /* internal */

    /**
     * Generate exception based of memcached result code
     *
     * @param int $code
     * @return Exception\RuntimeException
     * @throws Exception\InvalidArgumentException On success code
     */
    protected function getExceptionByResultCode($code)
    {
        switch ($code) {
            case MemcachedResource::RES_SUCCESS:
                throw new Exception\InvalidArgumentException(
                    "The result code '{$code}' (SUCCESS) isn't an error"
                );

            default:
                return new Exception\RuntimeException($this->getMemcachedResource()->getResultMessage());
        }
    }
}
