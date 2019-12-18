defmodule ElixirPopularity.HackerNewsIdGenerator do
  use GenServer

  require Logger

  alias ElixirPopularity.RMQPublisher

  def start_link(state) do
    GenServer.start_link(__MODULE__, state, name: __MODULE__)
  end

  @impl true
  def init(options) do
    state = Map.put(options, :timer_ref, nil)

    {:ok, state}
  end

  def start_generating, do: GenServer.call(__MODULE__, :start_generating)

  def stop_generating, do: GenServer.call(__MODULE__, :stop_generating)

  @impl true
  def handle_info(:poll_queue_size, %{current_id: current_id, end_id: end_id} = state)
      when current_id > end_id do
    Logger.info("No ids to generate")

    {:stop, :normal, state}
  end

  def handle_info(:poll_queue_size, state) do
    queue_size = RMQPublisher.hn_id_queue_size()

    new_current_id =
      if queue_size < state.generate_threshold do
        upper_range = min(state.current_id + state.batch_size, state.end_id)

        Logger.info("enqueuing items #{state.current_id} - #{upper_range}")

        state.current_id..upper_range
        |> Enum.each(fn hn_id ->
          RMQPublisher.publish_hn_id("#{hn_id}")
        end)

        upper_range + 1
      else
        Logger.info(
          "Queue size #{queue_size} is greater than the threshold #{state.generate_threshold}"
        )

        state.current_id
      end

    new_state =
      state
      |> Map.put(:current_id, new_current_id)
      |> Map.put(:timer_ref, schedule_next_poll(state.poll_rate))

    {:noreply, new_state}
  end

  @impl true
  def handle_call(:start_generating, _from, state) do
    send(self(), :poll_queue_size)

    {:reply, :ok, state}
  end

  def handle_call(:stop_generating, _from, state) do
    Process.cancel_timer(state.timer_ref)
    new_state = %{state | timer_ref: nil}

    {:reply, :ok, new_state}
  end

  defp schedule_next_poll(poll_rate) do
    Logger.info("Scheduling next poll")

    Process.send_after(self(), :poll_queue_size, poll_rate)
  end
end
