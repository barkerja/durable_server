defmodule DurableServer.EncryptionTest do
  use ExUnit.Case, async: true

  alias DurableServer.Encryption

  test "seals a value for multiple recipients" do
    {public_key1, private_key1} = Encryption.generate_key_pair()
    {public_key2, private_key2} = Encryption.generate_key_pair()

    assert {:ok, envelope} =
             Encryption.seal("sensitive state", [public_key1, public_key2], "server/123")

    assert Encryption.encrypted?(envelope)
    assert {:ok, "sensitive state"} = Encryption.open(envelope, private_key1, "server/123")
    assert {:ok, "sensitive state"} = Encryption.open(envelope, private_key2, "server/123")
  end

  test "rejects the wrong private key" do
    {public_key, _private_key} = Encryption.generate_key_pair()
    {_wrong_public_key, wrong_private_key} = Encryption.generate_key_pair()

    assert {:ok, envelope} = Encryption.seal("secret", [public_key], "server/123")

    assert {:error, :no_matching_recipient} =
             Encryption.open(envelope, wrong_private_key, "server/123")
  end

  test "authenticates the storage context" do
    {public_key, private_key} = Encryption.generate_key_pair()
    assert {:ok, envelope} = Encryption.seal("secret", [public_key], "server/123")

    assert {:error, :authentication_failed} =
             Encryption.open(envelope, private_key, "server/456")
  end

  test "rejects modified ciphertext" do
    {public_key, private_key} = Encryption.generate_key_pair()
    assert {:ok, envelope} = Encryption.seal("secret", [public_key], "server/123")

    ciphertext = Base.url_decode64!(envelope["ciphertext"], padding: false)
    <<first, rest::binary>> = ciphertext

    tampered =
      Map.put(
        envelope,
        "ciphertext",
        Base.url_encode64(<<Bitwise.bxor(first, 1), rest::binary>>, padding: false)
      )

    assert {:error, :authentication_failed} =
             Encryption.open(tampered, private_key, "server/123")
  end

  test "validates key and envelope shapes" do
    {public_key, private_key} = Encryption.generate_key_pair()

    assert {:error, :recipient_public_keys_required} = Encryption.seal("secret", [], "")

    assert {:error, {:invalid_recipient_public_key, 0}} =
             Encryption.seal("secret", [<<1, 2, 3>>], "")

    assert {:error, :invalid_decryption_key} =
             Encryption.open(%{"__durable_server_encrypted__" => 1}, <<1, 2, 3>>, "")

    assert {:ok, envelope} = Encryption.seal("secret", [public_key], "")

    assert {:error, {:unsupported_encryption_format, 2}} =
             envelope
             |> Map.put("__durable_server_encrypted__", 2)
             |> Encryption.open(private_key, "")
  end

  test "opens an HPKE recipient entry produced by superfly/ltx" do
    private_key =
      hex!("9803a596185df6e8097d620cd4e03c02f787b49f07333c5846d703da71199254")

    cek = hex!("4242424242424242424242424242424242424242424242424242424242424242")

    recipient_entry =
      hex!(
        "8a4ee371888306314799186d96f6579dfb2ec704d2cf9238f2dfc3a8172b826e" <>
          "f0901aa9cafc74d4877fb4aec2e3a6ac76b089da65fa4a06f07263372faee8bf" <>
          "2ed996020972eb7065be1a5c9496bb77"
      )

    context = "server/interop"
    plaintext = "cross-language"
    payload_key = :crypto.mac(:hmac, :sha256, cek, "durable-server-payload-key" <> <<1>>)
    nonce = <<0::96>>

    aad =
      <<"DSE1", 1, 0x0020::16, 0x0001::16, 0x0003::16, 1::16, recipient_entry::binary,
        byte_size(context)::32, context::binary>>

    {ciphertext, tag} =
      :crypto.crypto_one_time_aead(
        :chacha20_poly1305,
        payload_key,
        nonce,
        plaintext,
        aad,
        16,
        true
      )

    envelope = %{
      "__durable_server_encrypted__" => 1,
      "kem" => 0x0020,
      "kdf" => 0x0001,
      "aead" => 0x0003,
      "recipients" => [Base.url_encode64(recipient_entry, padding: false)],
      "nonce" => Base.url_encode64(nonce, padding: false),
      "ciphertext" => Base.url_encode64(ciphertext, padding: false),
      "tag" => Base.url_encode64(tag, padding: false)
    }

    assert {:ok, ^plaintext} = Encryption.open(envelope, private_key, context)
  end

  defp hex!(encoded), do: Base.decode16!(encoded, case: :lower)
end
