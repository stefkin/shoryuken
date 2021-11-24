module Shoryuken
  class Launcher
    include Util

    def initialize
      @managers = create_managers
    end

    def start
      logger.info { 'Starting' }

      start_callback
      start_managers
    end

    def stop!
      initiate_stop

      executor.shutdown

      return if executor.wait_for_termination(Shoryuken.options[:timeout])

      executor.kill
    end

    def stop
      fire_event(:quiet, true)

      initiate_stop

      executor.shutdown
      @managers.each { |m| m.own_executor.shutdown }
      executor.wait_for_termination
      @managers.each { |m| m.own_executor.wait_for_termination }
    end

    def healthy?
      Shoryuken.groups.keys.all? do |group|
        manager = @managers.find { |m| m.group == group }
        manager && manager.running?
      end
    end

    private

    def executor
      @_executor ||= begin
        # groups_concurrency = Shoryuken.groups.map do |_group, options|
        #   options[:concurrency]
        # end.sum

        Concurrent::ThreadPoolExecutor.new(
          min_threads: 1,
          max_threads: 25,
          auto_terminate: true,
          idletime: 60,
          max_queue: 100,
          fallback_policy: :abort
        )
      end
    end

    def start_managers
      @managers.each do |manager|
        Concurrent::Future.execute { manager.start }
      end
    end

    def initiate_stop
      logger.info { 'Shutting down' }

      stop_callback
    end

    def start_callback
      if (callback = Shoryuken.start_callback)
        logger.debug { 'Calling start_callback' }
        callback.call
      end

      fire_event(:startup)
    end

    def stop_callback
      if (callback = Shoryuken.stop_callback)
        logger.debug { 'Calling stop_callback' }
        callback.call
      end

      fire_event(:shutdown, true)
    end

    def create_managers
      Shoryuken.groups.map do |group, options|
        Shoryuken::Manager.new(
          group,
          Shoryuken::Fetcher.new(group),
          Shoryuken.polling_strategy(group).new(options[:queues], Shoryuken.delay(group)),
          options[:concurrency],
          executor
        )
      end
    end
  end
end
