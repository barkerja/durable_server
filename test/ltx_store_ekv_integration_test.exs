defmodule DurableServer.LTXStoreEKVIntegrationTest do
  use ExUnit.Case, async: false

  alias DurableServer.Backends.{EKVStore, EncryptedStore, LTXStore}
  alias DurableServer.Encryption

  @moduletag :integration
  @moduletag :capture_log

  test "LTXStore wrapping a managed EKV backend derives a plain EKV heartbeat store" do
    unique_id = System.unique_integer([:positive, :monotonic])
    ekv_name = :"durable_ltx_managed_ekv_#{unique_id}"
    heartbeat_name = :"#{ekv_name}_heartbeats"
    supervisor_name = :"durable_ltx_managed_ekv_supervisor_#{unique_id}"
    prefix = "ltx_ekv_managed/#{unique_id}/"
    data_dir = Path.join(System.tmp_dir!(), "durable_server_ltx_ekv_managed_#{unique_id}")

    File.rm_rf(data_dir)
    on_exit(fn -> File.rm_rf(data_dir) end)

    start_supervised!(%{
      id: {DurableServer.Supervisor, supervisor_name},
      start:
        {DurableServer.Supervisor, :start_link,
         [
           [
             name: supervisor_name,
             prefix: prefix,
             backend:
               {LTXStore,
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
                  page_size: 512
                ]},
             graceful_shutdown_timeout_ms: 500
           ]
         ]}
    })

    config = DurableServer.Supervisor.__get_config__(supervisor_name)

    assert config.storage_backend.adapter == LTXStore
    assert config.storage_backend.state.backend.adapter == EKVStore
    assert config.storage_backend.state.backend.state.name == ekv_name
    assert Process.whereis(:"#{ekv_name}_ekv_sup") != nil

    # Heartbeat derivation delegates through the LTX wrapper: the derived
    # store is a *plain* EKV store (heartbeats are tiny, constantly
    # rewritten, and gain nothing from LTX segment form), split into its own
    # data directory.
    assert config.heartbeat_backend.adapter == EKVStore
    assert config.heartbeat_backend.state.name == heartbeat_name
    assert Process.whereis(:"#{heartbeat_name}_ekv_sup") != nil
    assert File.dir?(Path.join(data_dir, "heartbeats"))
  end

  test "LTXStore over EncryptedStore over managed EKV derives an encrypted heartbeat store" do
    unique_id = System.unique_integer([:positive, :monotonic])
    ekv_name = :"durable_ltx_enc_managed_ekv_#{unique_id}"
    heartbeat_name = :"#{ekv_name}_heartbeats"
    supervisor_name = :"durable_ltx_enc_managed_ekv_supervisor_#{unique_id}"
    prefix = "ltx_enc_ekv_managed/#{unique_id}/"
    data_dir = Path.join(System.tmp_dir!(), "durable_server_ltx_enc_ekv_managed_#{unique_id}")

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
               {LTXStore,
                [
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
                  page_size: 512
                ]},
             graceful_shutdown_timeout_ms: 500
           ]
         ]}
    })

    config = DurableServer.Supervisor.__get_config__(supervisor_name)

    assert config.storage_backend.adapter == LTXStore
    assert config.storage_backend.state.backend.adapter == EncryptedStore
    assert config.storage_backend.state.backend.state.backend.adapter == EKVStore

    # Delegation preserves the security property: heartbeats stay encrypted
    # (EncryptedStore over EKV) without being LTX-wrapped, carrying forward
    # plaintext_compat.
    assert config.heartbeat_backend.adapter == EncryptedStore
    assert config.heartbeat_backend.state.backend.adapter == EKVStore
    assert config.heartbeat_backend.state.backend.state.name == heartbeat_name
    assert config.heartbeat_backend.state.plaintext_compat == :strict
    assert Process.whereis(:"#{heartbeat_name}_ekv_sup") != nil
  end
end
