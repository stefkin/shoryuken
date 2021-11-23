module Shoryuken
  class Manager
    include Util

    BATCH_LIMIT = 10
    # See https://github.com/phstc/shoryuken/issues/348#issuecomment-292847028
    MIN_DISPATCH_INTERVAL = 0.1

    attr_reader :group, :own_executor

    def initialize(group, fetcher, polling_strategy, concurrency, executor)
      @group            = group
      @fetcher          = fetcher
      @polling_strategy = polling_strategy
      @executor         = executor
      @max_processors   = executor.max_length
      @running          = Concurrent::AtomicBoolean.new(true)
      @own_executor = Concurrent::ThreadPoolExecutor.new(
        min_threads: 0,
        max_threads: concurrency,
        auto_terminate: true,
        idletime: 60,
        max_queue: 1,
        fallback_policy: :abort
      )
    end

    def start
      fire_utilization_update_event
      dispatch_loop
    end

    def running?
      @running.true? && @executor.running? && @own_executor.running?
    end

    private

    def dispatch_loop
      return unless running?

      @own_executor.post { dispatch }
    rescue Concurrent::RejectedExecutionError
      @executor.post { dispatch }
    end

    def dispatch
      return unless running?

      if (queue = @polling_strategy.next_queue).nil?
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
      @own_executor.length
    end

    def ready
      @max_processors - busy
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

      Concurrent::Promise
        .execute(executor: @own_executor) { Processor.process(queue_name, sqs_msg) }
        .then { processor_done(queue_name) }
        .rescue { processor_done(queue_name) }
    rescue Concurrent::RejectedExecutionError
      Concurrent::Promise
        .execute(executor: @executor) { Processor.process(queue_name, sqs_msg) }
        .then { processor_done(queue_name) }
        .rescue { processor_done(queue_name) }
    end

    def dispatch_batch(queue)
      batch = @fetcher.fetch(queue, BATCH_LIMIT)
      @polling_strategy.messages_found(queue.name, batch.size)
      assign(queue.name, patch_batch!(batch)) if batch.any?
    end

    def dispatch_single_messages(queue)
      messages = @fetcher.fetch(queue, ready)

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
  end
end
