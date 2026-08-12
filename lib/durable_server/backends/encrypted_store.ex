defmodule DurableServer.Backends.EncryptedStore do
  @moduledoc """
  Storage backend wrapper that encrypts values before delegating persistence.

  This wrapper can front any configured DurableServer backend. Existing
  plaintext objects remain readable and are encrypted the next time they are
  written.

  Configure it with a nested backend specification, one or more recipient
  public keys, and the private key for this deployment:

      {DurableServer.Backends.EncryptedStore,
       backend: {DurableServer.Backends.ObjectStore, object_store_opts},
       recipient_public_keys: [recipient_public_key],
       decryption_key: recipient_private_key}

  Recipient public keys can include both old and new keys during rotation. A
  write creates one wrapped content key per recipient, while any matching
  private key can read the value.
  """

  @behaviour DurableServer.StorageBackend

  alias DurableServer.{Encryption, Meta, StorageBackend, StoredState}

  @valid_opts [:backend, :recipient_public_keys, :decryption_key]
  @subscribe_ready_timeout_ms 5_000

  @type state :: %{
          required(:backend) => struct(),
          required(:recipient_public_keys) => [binary()],
          required(:decryption_key) => binary()
        }

  @impl true
  def init_backend(opts) when is_map(opts), do: opts |> Map.to_list() |> init_backend()

  def init_backend(opts) when is_list(opts) do
    opts = Keyword.validate!(opts, @valid_opts)
    backend = Keyword.fetch!(opts, :backend)
    recipient_public_keys = Keyword.fetch!(opts, :recipient_public_keys)
    decryption_key = Keyword.fetch!(opts, :decryption_key)

    cond do
      not match?(%StorageBackend{}, backend) ->
        raise ArgumentError,
              "encrypted backend :backend must be an initialized DurableServer.StorageBackend"

      not Encryption.supported?() ->
        {:error, :encryption_not_supported}

      true ->
        with {:ok, probe} <- Encryption.seal(<<>>, recipient_public_keys, <<>>),
             {:ok, <<>>} <- Encryption.open(probe, decryption_key, <<>>) do
          {:ok,
           %{
             state: %{
               backend: backend,
               recipient_public_keys: recipient_public_keys,
               decryption_key: decryption_key
             },
             defaults: StorageBackend.defaults(backend),
             features: StorageBackend.features(backend)
           }}
        end
    end
  end

  @impl true
  def ensure_ready(%{backend: backend}), do: StorageBackend.ensure_ready(backend)

  @impl true
  def get_object(%{} = state, key, opts) do
    case StorageBackend.get_object(state.backend, key, opts) do
      {:ok, %{body: body} = object} ->
        with {:ok, decoded} <- decode_stored_body(state, body, key) do
          {:ok, %{object | body: decoded}}
        end

      other ->
        other
    end
  end

  @impl true
  def list_all_objects_stream(%{} = state, prefix, opts) do
    error_handler = Keyword.get(opts, :error_handler, fn reason -> raise inspect(reason) end)

    state.backend
    |> StorageBackend.list_all_objects_stream(prefix, opts)
    |> Stream.transform(:ok, fn
      %{key: key, body: body} = object, :ok ->
        case decode_stored_body(state, body, key) do
          {:ok, decoded} ->
            {[%{object | body: decoded}], :ok}

          {:error, reason} ->
            case error_handler.({:decode_failed, key, reason}) do
              :halt -> {:halt, :ok}
              _other -> {[], :ok}
            end
        end

      object, :ok ->
        {[object], :ok}
    end)
  end

  @impl true
  def put_object(%{} = state, key, data, opts) do
    with {:ok, encrypted} <- encode_body(state, data, key) do
      case StorageBackend.put_object(state.backend, key, encrypted, opts) do
        {:ok, object} ->
          {:ok, %{object | body: data}}

        {:error, :conflict} ->
          resolve_ambiguous_conditional_put(state, key, data, opts)

        other ->
          other
      end
    end
  end

  @impl true
  def delete_object(%{} = state, key), do: StorageBackend.delete_object(state.backend, key)

  @impl true
  def delete_object(%{} = state, key, opts),
    do: StorageBackend.delete_object(state.backend, key, opts)

  @impl true
  def try_claim(%{} = state, key, body) do
    with {:ok, encrypted} <- encode_body(state, body, key) do
      StorageBackend.try_claim(state.backend, key, encrypted)
    end
  end

  @impl true
  def update_object(%{} = state, key, update_fn, opts) do
    result =
      StorageBackend.update_object(
        state.backend,
        key,
        fn %{body: encrypted, etag: etag} ->
          with {:ok, body} <- decode_stored_body(state, encrypted, key),
               {:ok, new_body} <- update_fn.(%{body: body, etag: etag}),
               {:ok, new_encrypted} <- encode_body(state, new_body, key) do
            {:ok, new_encrypted}
          end
        end,
        opts
      )

    case result do
      {:ok, %{body: encrypted} = object} ->
        with {:ok, body} <- decode_stored_body(state, encrypted, key) do
          {:ok, %{object | body: body}}
        end

      other ->
        other
    end
  end

  @impl true
  def encode(%{} = state, data), do: encode_body(state, data, <<>>)

  @impl true
  def decode(%{} = state, data) do
    if Encryption.encrypted?(data) do
      decode_encrypted_body(state, data, <<>>)
    else
      StorageBackend.decode(state.backend, data)
    end
  end

  @impl true
  def subscribe(%{} = state, subscriber, prefix, opts) do
    caller = self()

    relay_pid =
      spawn(fn ->
        subscription_relay_init(caller, subscriber, state, prefix, opts)
      end)

    monitor_ref = Process.monitor(relay_pid)

    receive do
      {:encrypted_store_subscribed, ^relay_pid, {:ok, inner_ref}} ->
        Process.demonitor(monitor_ref, [:flush])
        {:ok, {relay_pid, inner_ref}}

      {:encrypted_store_subscribed, ^relay_pid, {:error, reason}} ->
        Process.demonitor(monitor_ref, [:flush])
        {:error, reason}

      {:DOWN, ^monitor_ref, :process, ^relay_pid, reason} ->
        {:error, {:subscription_exit, reason}}
    after
      @subscribe_ready_timeout_ms ->
        Process.exit(relay_pid, :kill)
        {:error, :subscribe_timeout}
    end
  end

  @impl true
  def unsubscribe(%{} = _state, {relay_pid, inner_ref}) when is_pid(relay_pid) do
    if Process.alive?(relay_pid) do
      ref = make_ref()
      send(relay_pid, {:encrypted_store_unsubscribe, self(), ref, inner_ref})

      receive do
        {:encrypted_store_unsubscribed, ^ref} -> :ok
      after
        @subscribe_ready_timeout_ms -> :ok
      end
    else
      :ok
    end
  end

  def unsubscribe(%{} = _state, _subscription_ref), do: :ok

  defp encode_body(state, data, context) do
    with {:ok, encoded} <- StorageBackend.encode(state.backend, data),
         {:ok, plaintext} <- serialize(encoded) do
      Encryption.seal(plaintext, state.recipient_public_keys, context)
    end
  end

  defp decode_stored_body(state, body, context) do
    if Encryption.encrypted?(body) do
      decode_encrypted_body(state, body, context)
    else
      {:ok, body}
    end
  end

  defp decode_encrypted_body(state, envelope, context) do
    with {:ok, plaintext} <- Encryption.open(envelope, state.decryption_key, context),
         {:ok, encoded} <- deserialize(plaintext) do
      StorageBackend.decode(state.backend, encoded)
    end
  end

  defp serialize(term) do
    {:ok, :erlang.term_to_binary(term, compressed: 1)}
  rescue
    error in ArgumentError -> {:error, {:serialization_failed, error}}
  end

  defp deserialize(binary) do
    {:ok, :erlang.binary_to_term(binary, [:safe])}
  rescue
    error in ArgumentError -> {:error, {:invalid_encrypted_payload, error}}
  end

  defp resolve_ambiguous_conditional_put(
         state,
         key,
         %StoredState{meta: %Meta{} = attempted_meta} = attempted,
         opts
       ) do
    if Keyword.has_key?(opts, :etag) do
      case StorageBackend.get_object(state.backend, key, consistent: true) do
        {:ok, %{body: persisted_body, etag: etag}} ->
          with {:ok, persisted} <- decode_stored_body(state, persisted_body, key),
               %StoredState{meta: %Meta{} = persisted_meta} <- persisted,
               true <- same_boot_owner?(attempted_meta, persisted_meta),
               {:ok, attempted_encoded} <- StorageBackend.encode(state.backend, attempted),
               {:ok, persisted_encoded} <- StorageBackend.encode(state.backend, persisted),
               true <- attempted_encoded == persisted_encoded do
            {:ok, %{body: attempted, etag: etag}}
          else
            _other -> {:error, :conflict}
          end

        _other ->
          {:error, :conflict}
      end
    else
      {:error, :conflict}
    end
  end

  defp resolve_ambiguous_conditional_put(_state, _key, _data, _opts),
    do: {:error, :conflict}

  defp same_boot_owner?(
         %Meta{pid: pid, node_ref: node_ref, node_str: node_str},
         %Meta{pid: pid, node_ref: node_ref, node_str: node_str}
       )
       when is_pid(pid) and not is_nil(node_ref) and is_binary(node_str),
       do: true

  defp same_boot_owner?(%Meta{}, %Meta{}), do: false

  defp subscription_relay_init(caller, subscriber, state, prefix, opts) do
    case StorageBackend.subscribe(state.backend, self(), prefix, opts) do
      {:ok, inner_ref} ->
        send(caller, {:encrypted_store_subscribed, self(), {:ok, inner_ref}})
        subscription_relay_loop(subscriber, state, inner_ref)

      {:error, reason} ->
        send(caller, {:encrypted_store_subscribed, self(), {:error, reason}})
    end
  end

  defp subscription_relay_loop(subscriber, state, inner_ref) do
    receive do
      {:durable_server_storage_events, events} when is_list(events) ->
        decoded_events = Enum.flat_map(events, &decode_event(state, &1))

        if decoded_events != [] do
          send(subscriber, {:durable_server_storage_events, decoded_events})
        end

        subscription_relay_loop(subscriber, state, inner_ref)

      {:encrypted_store_unsubscribe, caller, ref, ^inner_ref} ->
        _ = StorageBackend.unsubscribe(state.backend, inner_ref)
        send(caller, {:encrypted_store_unsubscribed, ref})

      _other ->
        subscription_relay_loop(subscriber, state, inner_ref)
    end
  end

  defp decode_event(state, %{key: key, value: value} = event) when is_binary(key) do
    case decode_stored_body(state, value, key) do
      {:ok, decoded} -> [%{event | value: decoded}]
      {:error, _reason} -> []
    end
  end

  defp decode_event(_state, event), do: [event]
end
