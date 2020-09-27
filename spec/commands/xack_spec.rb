require 'spec_helper'

describe '#xack(key, group, *ids)' do
  let(:key) { 'mock-redis-test:xack-key' }
  let(:group) { 'mock-redis-test:xack-group' }
  let(:consumer) { 'mock-redis-test:xack-consumer' }

  after :each do
    @redises.del(key)
  end

  it 'acknowledges a single entry' do
    @redises.xgroup(:create, key, group, '$', mkstream: true)
    @redises.xadd(key, { key: 'value' }, id: '1234567891234-0')
    @redises.xreadgroup(group, consumer, key, '>')

    expect(@redises.xack(key, group, '1234567891234-0')).to eq 1
    expect(@redises.xreadgroup(group, consumer, key, '>')).to eq({})
  end

  it 'does not acknowledge an entry that has not been read' do
    @redises.xgroup(:create, key, group, '$', mkstream: true)
    @redises.xadd(key, { key: 'value' }, id: '1234567891234-0')

    expect(@redises.xack(key, group, '1234567891234-0')).to eq 0
    expect(@redises.xreadgroup(group, consumer, key, '>'))
      .to eq({ key => [['1234567891234-0', { 'key' => 'value' }]] })
  end
end