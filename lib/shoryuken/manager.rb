module Shoryuken
  class Manager
    include Util

    BATCH_LIMIT = 10
    # See https://github.com/phstc/shoryuken/issues/348#issuecomment-292847028
    MIN_DISPATCH_INTERVAL = 0.1

    attr_reader :group, :own_executor

    def initialize(group, fetcher, polling_strategy, concurrency, shared_executor)
      @group            = group
      @fetcher          = fetcher
      @polling_strategy = polling_strategy
      @shared_executor = shared_executor
      @max_processors   = shared_executor.max_length
      @running          = Concurrent::AtomicBoolean.new(true)
      init_own_executor(concurrency) if concurrency&.positive?
    end

    def start
      fire_utilization_update_event
      dispatch_loop
    end

    def running?
      @running.true? && @shared_executor.running? && (own_executor.nil? || own_executor.running?)
    end

    private

    def dispatch_loop
      return unless running?

      with_executor do |executor|
        executor.post { dispatch }
      end
    end

    def with_executor
      return yield(@shared_executor) if own_executor.nil?

      begin
        yield(own_executor)
      rescue Concurrent::RejectedExecutionError
        yield(@shared_executor)
      end
    end

    def dispatch
      return unless running?

      if ready < 1 || (queue = @polling_strategy.next_queue).nil?
        return sleep(MIN_DISPATCH_INTERVAL)
      end

      fire_event(:dispatch, false, queue_name: queue.name)

      logger.debug { "Ready: #{ready}, Busy: #{busy}, Active Queues: #{@polling_strategy.active_queues}" }

      batched_queue?(queue) ? dispatch_batch(queue) : dispatch_single_messages(queue)
    rescue => e
      handle_dispatch_error(e)
    ensure
      dispatch_loop
    end

    def busy
      @own_executor&.length || 0
    end

    def ready
      @shared_executor.remaining_capacity + (own_executor&.remaining_capacity || 0)
    end

    def processor_done(queue)
      fire_utilization_update_event

      client_queue = Shoryuken::Client.queues(queue)
      return unless client_queue.fifo?
      return unless @polling_strategy.respond_to?(:message_processed)

      @polling_strategy.message_processed(queue)
    end

    def assign(queue_name, sqs_msg)
      return unless running?

      logger.debug { "Assigning #{sqs_msg.message_id}" }

      fire_utilization_update_event

      with_executor do |executor|
        Concurrent::Promise
          .execute(executor: executor) { Processor.process(queue_name, sqs_msg) }
          .then { processor_done(queue_name) }
          .rescue { processor_done(queue_name) }
      end
    end

    def dispatch_batch(queue)
      batch = @fetcher.fetch(queue, BATCH_LIMIT)
      @polling_strategy.messages_found(queue.name, batch.size)
      assign(queue.name, patch_batch!(batch)) if batch.any?
    end

    def dispatch_single_messages(queue)
      messages = @fetcher.fetch(queue, nil)

      @polling_strategy.messages_found(queue.name, messages.size)
      messages.each { |message| assign(queue.name, message) }
    end

    def batched_queue?(queue)
      Shoryuken.worker_registry.batch_receive_messages?(queue.name)
    end

    def patch_batch!(sqs_msgs)
      sqs_msgs.instance_eval do
        def message_id
          "batch-with-#{size}-messages"
        end
      end

      sqs_msgs
    end

    def handle_dispatch_error(ex)
      logger.error { "Manager failed: #{ex.message}" }
      logger.error { ex.backtrace.join("\n") } unless ex.backtrace.nil?

      Process.kill('USR1', Process.pid)

      @running.make_false
    end

    def fire_utilization_update_event
      fire_event :utilization_update, false, {
        group: @group,
        max_processors: @max_processors,
        busy_processors: busy
      }
    end

    def init_own_executor(concurrency)
      Rails.logger.info(event: :shoryuken_start, concurrency: concurrency, executor: @group)

      @own_executor = Concurrent::ThreadPoolExecutor.new(
        min_threads: 0,
        max_threads: concurrency,
        auto_terminate: true,
        idletime: 60,
        max_queue: -1,
        fallback_policy: :abort
      )
    end
  end
end
