class MockRedis
  class Stream
    class Group
      def initialize
        @assigned = {}
        @allocated = []
      end

      def read(members, consumer, _id, *_opts_in)
        existing = assigned_for(consumer)
        return existing.map { |m| [m[0].to_s, m[1]] } if existing

        items = members.reject { |m| @allocated.include? m[0] }.first(1)
        @allocated += items.map(&:first)
        assign_to(consumer, items)

        items.map { |m| [m[0].to_s, m[1]] }
      end

      def ack(*ids)
        matching_ids = @allocated.map(&:to_s) & ids
        consumers.each do |consumer|
          @assigned[consumer].reject! { |assigned| ids.include? assigned[0].to_s }
        end

        matching_ids.count
      end

      private

      def assigned_for(consumer)
        @assigned[consumer]
      end

      def assign_to(consumer, items)
        @assigned[consumer] = items
      end

      def consumers
        @assigned.keys
      end
    end
  end
end
