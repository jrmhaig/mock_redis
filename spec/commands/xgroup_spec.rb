require 'spec_helper'

describe '#xgroup(subcommand, key, group, id_or_consumer = nil, mkstream: false)' do
  let(:key) { 'mock-redis-test:xgroup-key' }
  let(:group) { 'mock-redis-test:xgroup-group' }

  before :each do
    @redises._gsub(/\d{3}-\d/, '...-.')
    Timecop.freeze
  end

  after :each do
    @redises.del(key)
    Timecop.return
  end

  context 'subcommand :create' do
    it 'does not create a group if the stream does not exist' do
      expect do
        @redises.xgroup(:create, key, group, '$')
      end.to raise_error(
        Redis::CommandError,
        'ERR The XGROUP subcommand requires the key to exist. Note that for CREATE you may want ' \
        'to use the MKSTREAM option to create an empty stream automatically.'
      )
    end

    it 'creates a group on an existing stream' do
      @redises.xadd(key, { key: 'value' })
      expect(@redises.xgroup(:create, key, group, '$')).to eq 'OK'
      @redises.del(key)
    end

    it 'succeeds on a stream that does not exist if if mkstream is true' do
      expect(@redises.xgroup(:create, key, group, '$', mkstream: true)).to eq 'OK'
      @redises.del(key)
    end

    it 'raises an error if the group already exists' do
      @redises.xgroup(:create, key, group, '$', mkstream: true)
      expect do
        @redises.xgroup(:create, key, group, '$', mkstream: true)
      end.to raise_error(
        Redis::CommandError,
        'BUSYGROUP Consumer Group name already exists'
      )
    end

    it 'sets all items as delivered with id $' do
      @redises.xadd(key, { key: 'first value' }, id: '1234567891234-0')
      @redises.xgroup(:create, key, group, '$', mkstream: true)
      expect(@redises.xreadgroup(group, 'consumer', key, '>')).to eq({})
    end

    it 'sets items at or before a given id as delivered' do
      @redises.xadd(key, { key: 'first value' }, id: '1234567891234-0')
      @redises.xadd(key, { key: 'second value' }, id: '1234567891235-0')
      @redises.xadd(key, { key: 'third value' }, id: '1234567891235-1')
      @redises.xgroup(:create, key, group, '1234567891235-0', mkstream: true)
      expect(@redises.xreadgroup(group, 'consumer', key, '>'))
        .to eq({ key => [['1234567891235-1', { 'key' => 'third value' }]] })
    end
  end

  context 'subcommand :setid' do
  end

  context 'subcommand :destroy' do
  end

  context 'subcommand :delconsumer' do
  end
end
