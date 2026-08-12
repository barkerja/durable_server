defmodule DurableServer.EncryptedStoreEKVIntegrationTest do
  use ExUnit.Case, async: false

  alias DurableServer.Backends.{EKVStore, EncryptedStore}
  alias DurableServer.Encryption

  @moduletag :integration
  @moduletag :capture_log

  test "EncryptedStore wrapping a managed EKV backend starts the managed child and derives an EncryptedStore-wrapped heartbeat backend that inherits plaintext_compat" do
    unique_id = System.unique_integer([:positive, :monotonic])
    ekv_name = :"durable_encrypted_managed_ekv_#{unique_id}"
    heartbeat_name = :"#{ekv_name}_heartbeats"
    supervisor_name = :"durable_encrypted_managed_ekv_supervisor_#{unique_id}"
    prefix = "encrypted_ekv_managed/#{unique_id}/"
    data_dir = Path.join(System.tmp_dir!(), "durable_server_encrypted_ekv_managed_#{unique_id}")

    File.rm_rf(data_dir)
    on_exit(fn -> File.rm_rf(data_dir) end)

    {public_key, private_key} = Encryption.generate_key_pair()

    start_supervised!(%{
      id: {DurableServer.Supervisor, supervisor_name},
      start:
        {DurableServer.Supervisor, :start_link,
         [
           [
             name: supervisor_name,
             prefix: prefix,
             backend:
               {EncryptedStore,
                [
                  backend:
                    {EKVStore,
                     [
                       name: ekv_name,
                       data_dir: data_dir,
                       cluster_size: 1,
                       node_id: 1,
                       log: false
                     ]},
                  recipient_public_keys: [public_key],
                  decryption_key: private_key,
                  plaintext_compat: :strict
                ]},
             graceful_shutdown_timeout_ms: 500
           ]
         ]}
    })

    config = DurableServer.Supervisor.__get_config__(supervisor_name)

    # F7: init_backend_resource's EncryptedStore clause previously handed the raw nested EKVStore
    # opts straight to init_backend! without routing them through the managed-EKV path, which
    # crashed start_link with "unknown keys [:node_id, :cluster_size, :data_dir]". Assert the
    # managed EKV child is genuinely running, not just that init/1 didn't raise.
    assert config.storage_backend.adapter == EncryptedStore
    assert config.storage_backend.state.backend.adapter == EKVStore
    assert config.storage_backend.state.backend.state.name == ekv_name
    assert Process.whereis(:"#{ekv_name}_ekv_sup") != nil

    # F7b / G3: the auto-derived heartbeat backend must itself be EncryptedStore-wrapped, and
    # must carry forward the primary store's plaintext_compat instead of silently defaulting to
    # :permissive.
    assert config.heartbeat_backend.adapter == EncryptedStore
    assert config.heartbeat_backend.state.backend.adapter == EKVStore
    assert config.heartbeat_backend.state.backend.state.name == heartbeat_name
    assert config.heartbeat_backend.state.plaintext_compat == :strict
    assert Process.whereis(:"#{heartbeat_name}_ekv_sup") != nil

    assert File.dir?(Path.join(data_dir, "heartbeats"))
  end
end
