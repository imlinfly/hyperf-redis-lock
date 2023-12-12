<?php

/**
 * Created by PhpStorm.
 * User: LinFei
 * Created time 2023/12/15 15:08:10
 * E-mail: fly@eyabc.cn
 */
declare (strict_types=1);

namespace Lynnfly\HyperfRedisLock;

use Hyperf\Context\ApplicationContext;
use Hyperf\Coordinator\Timer;
use Hyperf\Coroutine\Coroutine;
use Hyperf\Redis\Redis;
use Hyperf\Redis\RedisFactory;

class Lock
{
    /**
     * 加锁的key
     * @var string
     */
    protected string $key;

    /**
     * 锁的ID
     * @var string
     */
    protected string $lockId;

    /**
     * 是否加锁
     * @var bool
     */
    protected bool $isLock = false;

    /**
     * 定时器ID
     * @var int
     */
    protected int $timerId = 0;

    /**
     * 定时器
     * @var Timer
     */
    protected Timer $timer;

    /**
     * @param string $key 锁的key
     * @param int $expire 锁的有效期
     * @param string $prefix 锁的前缀
     * @param string $redisPool redis连接池
     */
    public function __construct(
        string           $key,
        protected int    $expire = 5,
        string           $prefix = 'redis_lock:',
        protected string $redisPool = 'default',
    )
    {
        $this->key = $prefix . $key;
        $this->lockId = md5(getmypid() . uniqid() . microtime(true) . mt_rand(1000, 9999));

        $this->timer = ApplicationContext::getContainer()->get(Timer::class);
    }

    /**
     * 加锁
     *
     * @param callable|null $callback 加锁成功后的回调函数 使用该参数会自动解锁
     * @param bool $renewal 是否需要锁续命
     * @param bool $wait 是否等待加锁
     * @return bool|mixed
     */
    public function lock(callable $callback = null, bool $renewal = true, bool $wait = false): mixed
    {
        $redis = $this->getRedis();

        while (true) {
            // 写入锁
            $this->isLock = $redis->set($this->key, $this->lockId, ['NX', 'EX' => $this->expire]);

            if ($this->isLock) {
                break;
            }
            if (!$wait) {
                return false;
            }

            // 睡眠250毫秒
            Coroutine::sleep(0.25);
        }

        // 定时器间隔时间 有效期的一半
        $timeInterval = max(1, $this->expire / 2);

        if ($renewal) {
            $this->timerId = $this->timer->tick($timeInterval, function () use ($redis) {
                if (!$this->isLock) {
                    // 如果已经解锁，则删除定时器
                    $this->timer->clear($this->timerId);
                } else {
                    // 如果没有解锁，则延长锁的有效期
                    $redis->expire($this->key, $this->expire);
                }
            });
        }

        if (!is_callable($callback)) {
            return $this->isLock;
        }

        try {
            return $callback();
        } finally {
            $this->unlock();
        }
    }

    /**
     * 解锁
     * @return bool
     */
    public function unlock(): bool
    {
        if (!$this->isLock) {
            return true;
        }

        $script = <<<LUA
if redis.call("get",KEYS[1]) == ARGV[1] then
    return redis.call("del",KEYS[1])
else
    return -1
end
LUA;

        $result = (int)$this->getRedis()->eval($script, [$this->key, $this->lockId], 1);

        // 不管是否解锁成功，都删除定时器
        $this->deleteTimer();

        if ($result > 0 || $result === -1) {
            $this->isLock = false;
            return true;
        }

        return false;
    }

    /**
     * 获取redis实例
     * @return Redis
     */
    protected function getRedis(): Redis
    {
        return ApplicationContext::getContainer()
            ->get(RedisFactory::class)
            ->get($this->redisPool);
    }

    /**
     * 删除定时器
     * @return void
     */
    protected function deleteTimer(): void
    {
        if ($this->timerId) {
            $this->getRedis()->del($this->timerId);
            $this->timerId = 0;
        }
    }

    public function __destruct()
    {
        $this->deleteTimer();
    }
}
