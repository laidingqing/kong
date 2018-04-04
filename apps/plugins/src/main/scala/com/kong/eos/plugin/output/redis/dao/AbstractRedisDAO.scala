
package com.kong.eos.plugin.output.redis.dao

import com.redis.RedisClientPool
import com.redis.serialization.{Format, Parse}

/**
 * Trait with common operations over redis server.
 *
 */
trait AbstractRedisDAO {

  def hostname: String

  def port: Int

  val IdSeparator: String = ":"

  val DefaultRedisPort: String = "6379"

  val DefaultRedisHostname: String = "localhost"

  protected def pool: RedisClientPool = AbstractRedisDAO.pool(hostname, port)

  def hset(key: Any, field: Any, value: Any)(implicit format: Format): Boolean =
    pool.withClient(client => client.hset(key, field, value))


  def hget[A](key: Any, field: Any)(implicit format: Format, parse: Parse[A]): Option[A] = {
    pool.withClient(client =>
      client.hget(key, field)
    )
  }
}

/**
 * Initializes singletons objects needed in the trait.
 *
 * @author anistal
 */
private object AbstractRedisDAO {

  var instance: Option[RedisClientPool] = None

  /**
   * Initializes a Redis connection pool.
   *
   * @param hostname of the redis server.
   * @param port of the redis server.
   * @return a pool of connections.
   */
  def pool(hostname: String, port: Int): RedisClientPool = {
    instance = if(instance.isEmpty) Some(new RedisClientPool(hostname, port)) else instance
    instance.get
  }
}
