require 'mock_redis/assertions'
require 'mock_redis/utility_methods'
require 'mock_redis/stream'

# TODO: Implement the following commands
#
#   * xgroup
#   * xreadgroup
#   * xack
#   * xpending
#   * xclaim
#   * xinfo
#   * xtrim
#   * xdel
#
# TODO: Complete support for
#
#   * xtrim
#       - `approximate: true` argument is currently ignored
#   * xadd
#       - `approximate: true` argument (for capped streams) is currently ignored
#
# For details of these commands see
#   * https://redis.io/topics/streams-intro
#   * https://redis.io/commands#stream

class MockRedis
  module StreamMethods
    include Assertions
    include UtilityMethods

    def xadd(key, entry, opts = {})
      id = opts[:id] || '*'
      with_stream_at(key) do |stream|
        stream.add id, entry
        stream.trim opts[:maxlen] if opts[:maxlen]
        return stream.last_id
      end
    end

    def xtrim(key, count)
      with_stream_at(key) do |stream|
        stream.trim count
      end
    end

    def xlen(key)
      with_stream_at(key) do |stream|
        return stream.count
      end
    end

    def xrange(key, first = '-', last = '+', count: nil)
      args = [first, last, false]
      args += ['COUNT', count] if count
      with_stream_at(key) do |stream|
        return stream.range(*args)
      end
    end

    def xrevrange(key, last = '+', first = '-', count: nil)
      args = [first, last, true]
      args += ['COUNT', count] if count
      with_stream_at(key) do |stream|
        return stream.range(*args)
      end
    end

    def xread(keys, ids, count: nil, block: nil)
      args = []
      args += ['COUNT', count] if count
      args += ['BLOCK', block.to_i] if block
      result = {}
      keys = keys.is_a?(Array) ? keys : [keys]
      ids = ids.is_a?(Array) ? ids : [ids]
      keys.each_with_index do |key, index|
        with_stream_at(key) do |stream|
          data = stream.read(ids[index], *args)
          result[key] = data unless data.empty?
        end
      end
      result
    end

    def xgroup(subcommand, key, group, id_or_consumer, mkstream: false)
      if mkstream || data[key].is_a?(Stream)
        with_stream_at(key) do |stream|
          # TODO
        end
        'OK'
      else
        raise Redis::CommandError,
        'ERR The XGROUP subcommand requires the key to exist. Note that for CREATE you may want ' \
        'to use the MKSTREAM option to create an empty stream automatically.'
      end
    end

    private

    def with_stream_at(key, &blk)
      with_thing_at(key, :assert_streamy, proc { Stream.new }, &blk)
    end

    def streamy?(key)
      data[key].nil? || data[key].is_a?(Stream)
    end

    def assert_streamy(key)
      unless streamy?(key)
        raise Redis::CommandError,
          'WRONGTYPE Operation against a key holding the wrong kind of value'
      end
    end
  end
end
