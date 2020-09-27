require 'spec_helper'

describe '#xreadgroup(group, consumer, keys, ids, count: nil, block: nil, noack: nil)' do
  let(:key) { 'mock-redis-test:xreadgroup-key' }
  let(:group) { 'mock-redis-test:xreadgroup-group' }
  let(:consumer) { 'mock-redis-test:xreadgroup-consumer' }
  let(:consumer2) { 'mock-redis-test:xreadgroup-consumer2' }

  before :each do
    @redises._gsub(/\d{3}-\d/, '...-.')
    @redises.xgroup(:create, key, group, '$', mkstream: true)
  end

  after :each do
    @redises.del(key)
  end

  it 'reads a single entry' do
    @redises.xadd(key, { key: 'value' }, id: '1234567891234-0')

    expect(@redises.xreadgroup(group, consumer, key, '>'))
      .to eq({ key => [['1234567891234-0', { 'key' => 'value' }]] })
  end

  it 'does not allow two consumers to have the same event' do
    @redises.xadd(key, { key: 'value' }, id: '1234567891234-0')
    @redises.xreadgroup(group, consumer, key, '>')

    expect(@redises.xreadgroup(group, consumer2, key, '>')).to eq({})
  end

  it 'reads an unacknowledged event allocated to the same consumer' do
    @redises.xadd(key, { key: 'value' }, id: '1234567891234-0')
    @redises.xreadgroup(group, consumer, key, '>')

    expect(@redises.xreadgroup(group, consumer, key, '0'))
      .to eq({ key => [['1234567891234-0', { 'key' => 'value' }]] })
  end

  it 'does not read for an unknown group' do
    @redises.xadd(key, { key: 'value' }, id: '1234567891234-0')
    expect do
      @redises.xreadgroup('unknown-group', consumer, key, '>')
    end.to raise_error(
      Redis::CommandError,
      "NOGROUP No such key '#{key}' or consumer group 'unknown-group' " \
      'in XREADGROUP with GROUP option'
    )
  end
end
