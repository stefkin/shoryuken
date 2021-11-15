module Shoryuken
  class Launcher
    include Util

    def initialize
      @managers = create_managers
      @busy_processors = Concurrent::AtomicFixnum.new(0)
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
      executor.wait_for_termination
    end

    def healthy?
      Shoryuken.groups.keys.all? do |group|
        manager = @managers.find { |m| m.group == group }
        manager && manager.running?
      end
    end

    private

    def executor
      @_executor ||= Shoryuken.launcher_executor || Concurrent.global_io_executor
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
          Shoryuken.options[:concurrency],
          executor,
          @busy_processors
        )
      end
    end
  end
end
