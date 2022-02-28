defmodule CrissCrossDHT.NameWatcher do
  use GenServer

  require Logger

  import CrissCrossDHT.Server.Utils

  def start_link(path) do
    GenServer.start_link(__MODULE__, path, name: __MODULE__)
  end

  def init(path) do
    {:ok, watcher_pid} = FileSystem.start_link(dirs: [path])
    FileSystem.subscribe(watcher_pid)
    names = read_names(path)
    {:ok, %{watcher_pid: watcher_pid, names: names}}
  end

  def get_name(name_id),
    do: GenServer.call(__MODULE__, {:get_name, name_id}, :infinity)

  def get_names(),
    do: GenServer.call(__MODULE__, :get_names, :infinity)

  def handle_call(
        {:get_name, name_id},
        _,
        %{names: names} = state
      ) do
    {:reply, Map.get(names, name_id), state}
  end

  def handle_call(
        :get_names,
        _,
        %{names: names} = state
      ) do
    {:reply, names, state}
  end

  def handle_info(
        {:file_event, watcher_pid, {path, events}},
        %{watcher_pid: watcher_pid, names: names} = state
      ) do
    if String.ends_with?(path, "yaml") and :modified in events do
      Logger.info("Name config change detected: #{path} #{inspect(events)}")
      new_names = read_name(path) |> Enum.into(names)
      {:noreply, %{state | names: new_names}}
    else
      {:noreply, state}
    end
  end

  def handle_info({:file_event, watcher_pid, :stop}, %{watcher_pid: watcher_pid} = state) do
    {:noreply, state}
  end

  def read_names(name_dir) do
    Path.wildcard("#{name_dir}/*.yaml")
    |> Enum.flat_map(fn name ->
      Logger.info("Loading name config: #{name}")
      read_name(name)
    end)
    |> Enum.into(%{})
  end

  def read_name(name_file) do
    case YamlElixir.read_from_string(File.read!(name_file)) do
      {:ok,
       %{
         "Name" => name,
         "PrivateKey" => priv,
         "PublicKey" => pubk
       }} ->
        with {:ok, pub_key} <- load_public_key(decode_human!(pubk)),
             {:ok, priv_key} <- load_private_key(decode_human!(priv)) do
          [
            {decode_human!(name),
             %{
               public_key: pub_key,
               private_key: priv_key
             }}
          ]
        else
          e ->
            Logger.error("Invalid key in #{name_file} #{inspect(e)}")
            []
        end

      _ ->
        Logger.error("Invalid yaml file #{name_file}")
        []
    end
  end
end
