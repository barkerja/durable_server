defmodule DurableServer.Encryption do
  @moduledoc """
  Authenticated envelope encryption for DurableServer storage backends.

  Each encrypted value uses a fresh content-encryption key (CEK). The CEK is
  wrapped independently for every configured recipient using RFC 9180 HPKE
  with DHKEM(X25519, HKDF-SHA256), HKDF-SHA256, and ChaCha20-Poly1305. The
  value itself is encrypted with a key derived from the CEK.

  Keys are raw 32-byte X25519 keys. Encode them before placing them in text
  configuration such as environment variables.
  """

  @format_version 1
  @marker "__durable_server_encrypted__"
  @magic "DSE1"
  @kem_id 0x0020
  @kdf_id 0x0001
  @aead_id 0x0003
  @key_size 32
  @nonce_size 12
  @tag_size 16
  @recipient_entry_size 80
  @max_recipients 32
  @hpke_version "HPKE-v1"
  @kem_suite_id <<"KEM", @kem_id::16>>
  @hpke_suite_id <<"HPKE", @kem_id::16, @kdf_id::16, @aead_id::16>>
  @cek_info "ltx-cek-wrap"
  @payload_key_info "durable-server-payload-key"

  @type envelope :: %{required(String.t()) => term()}

  @doc """
  Generates a raw X25519 `{public_key, private_key}` pair.
  """
  @spec generate_key_pair() :: {binary(), binary()}
  def generate_key_pair do
    :crypto.generate_key(:ecdh, :x25519)
  end

  @doc false
  @spec supported?() :: boolean()
  def supported? do
    :x25519 in :crypto.supports(:curves) and
      :chacha20_poly1305 in :crypto.supports(:ciphers)
  end

  @doc false
  @spec encrypted?(term()) :: boolean()
  def encrypted?(%{@marker => _version}), do: true
  def encrypted?(_term), do: false

  @doc false
  @spec seal(binary(), [binary()], binary()) :: {:ok, envelope()} | {:error, term()}
  def seal(plaintext, recipient_public_keys, context \\ "")
      when is_binary(plaintext) and is_list(recipient_public_keys) and is_binary(context) do
    with :ok <- validate_recipient_keys(recipient_public_keys),
         cek <- :crypto.strong_rand_bytes(@key_size),
         {:ok, recipient_entries} <- seal_cek(cek, recipient_public_keys),
         {:ok, payload_key} <- hkdf_expand(cek, @payload_key_info, @key_size),
         nonce <- :crypto.strong_rand_bytes(@nonce_size),
         aad <- envelope_aad(recipient_entries, context),
         {ciphertext, tag} <-
           :crypto.crypto_one_time_aead(
             :chacha20_poly1305,
             payload_key,
             nonce,
             plaintext,
             aad,
             @tag_size,
             true
           ) do
      {:ok,
       %{
         @marker => @format_version,
         "kem" => @kem_id,
         "kdf" => @kdf_id,
         "aead" => @aead_id,
         "recipients" => Enum.map(recipient_entries, &encode64/1),
         "nonce" => encode64(nonce),
         "ciphertext" => encode64(ciphertext),
         "tag" => encode64(tag)
       }}
    end
  rescue
    error in ErlangError -> {:error, {:encryption_failed, rescued_reason(error)}}
  end

  @doc false
  @spec open(envelope(), binary(), binary()) :: {:ok, binary()} | {:error, term()}
  def open(envelope, private_key, context \\ "")

  def open(envelope, private_key, context)
      when is_map(envelope) and is_binary(private_key) and is_binary(context) do
    with :ok <- validate_private_key(private_key),
         {:ok, recipient_entries, nonce, ciphertext, tag} <- decode_envelope(envelope),
         {:ok, cek} <- open_cek(recipient_entries, private_key),
         {:ok, payload_key} <- hkdf_expand(cek, @payload_key_info, @key_size),
         aad <- envelope_aad(recipient_entries, context),
         plaintext when is_binary(plaintext) <-
           :crypto.crypto_one_time_aead(
             :chacha20_poly1305,
             payload_key,
             nonce,
             ciphertext,
             aad,
             tag,
             false
           ) do
      {:ok, plaintext}
    else
      :error -> {:error, :authentication_failed}
      {:error, _reason} = error -> error
    end
  rescue
    error in ErlangError -> {:error, {:decryption_failed, rescued_reason(error)}}
  end

  def open(_envelope, _private_key, _context), do: {:error, :invalid_envelope}

  defp validate_recipient_keys([]), do: {:error, :recipient_public_keys_required}

  defp validate_recipient_keys(recipient_public_keys)
       when length(recipient_public_keys) <= @max_recipients do
    case Enum.find_index(recipient_public_keys, &(not valid_key?(&1))) do
      nil -> :ok
      index -> {:error, {:invalid_recipient_public_key, index}}
    end
  end

  defp validate_recipient_keys(_recipient_public_keys), do: {:error, :too_many_recipients}

  defp validate_private_key(private_key) do
    if valid_key?(private_key), do: :ok, else: {:error, :invalid_decryption_key}
  end

  defp valid_key?(key), do: is_binary(key) and byte_size(key) == @key_size

  defp seal_cek(cek, recipient_public_keys) do
    recipient_public_keys
    |> Enum.with_index()
    |> Enum.reduce_while({:ok, []}, fn {public_key, index}, {:ok, entries} ->
      case hpke_seal(public_key, @cek_info, cek) do
        {:ok, entry} -> {:cont, {:ok, [entry | entries]}}
        {:error, _reason} -> {:halt, {:error, {:invalid_recipient_public_key, index}}}
      end
    end)
    |> case do
      {:ok, entries} -> {:ok, Enum.reverse(entries)}
      {:error, _reason} = error -> error
    end
  end

  defp open_cek(recipient_entries, private_key) do
    {recipient_public_key, ^private_key} = :crypto.generate_key(:ecdh, :x25519, private_key)

    Enum.reduce_while(recipient_entries, {:error, :no_matching_recipient}, fn entry, _acc ->
      case hpke_open(entry, private_key, recipient_public_key, @cek_info) do
        {:ok, cek} -> {:halt, {:ok, cek}}
        {:error, _reason} -> {:cont, {:error, :no_matching_recipient}}
      end
    end)
  end

  defp hpke_seal(recipient_public_key, info, plaintext) do
    {encapsulated_key, ephemeral_private_key} = :crypto.generate_key(:ecdh, :x25519)

    with {:ok, shared_secret} <-
           dhkem_shared_secret(
             recipient_public_key,
             ephemeral_private_key,
             encapsulated_key,
             recipient_public_key
           ),
         {:ok, key, nonce} <- hpke_key_schedule(shared_secret, info),
         {ciphertext, tag} <-
           :crypto.crypto_one_time_aead(
             :chacha20_poly1305,
             key,
             nonce,
             plaintext,
             <<>>,
             @tag_size,
             true
           ) do
      {:ok, encapsulated_key <> ciphertext <> tag}
    end
  rescue
    error in ErlangError -> {:error, {:hpke_seal_failed, rescued_reason(error)}}
  end

  defp hpke_open(
         <<encapsulated_key::binary-size(@key_size), ciphertext::binary-size(@key_size),
           tag::binary-size(@tag_size)>>,
         recipient_private_key,
         recipient_public_key,
         info
       ) do
    with {:ok, shared_secret} <-
           dhkem_shared_secret(
             encapsulated_key,
             recipient_private_key,
             encapsulated_key,
             recipient_public_key
           ),
         {:ok, key, nonce} <- hpke_key_schedule(shared_secret, info),
         plaintext when is_binary(plaintext) <-
           :crypto.crypto_one_time_aead(
             :chacha20_poly1305,
             key,
             nonce,
             ciphertext,
             <<>>,
             tag,
             false
           ) do
      {:ok, plaintext}
    else
      :error -> {:error, :hpke_authentication_failed}
      {:error, _reason} = error -> error
    end
  rescue
    error in ErlangError -> {:error, {:hpke_open_failed, rescued_reason(error)}}
  end

  defp hpke_open(_entry, _recipient_private_key, _recipient_public_key, _info),
    do: {:error, :invalid_recipient_entry}

  defp dhkem_shared_secret(peer_public_key, private_key, encapsulated_key, recipient_public_key) do
    shared_dh = :crypto.compute_key(:ecdh, peer_public_key, private_key, :x25519)

    if shared_dh == <<0::256>> do
      {:error, :invalid_x25519_public_key}
    else
      eae_prk = labeled_extract(<<>>, @kem_suite_id, "eae_prk", shared_dh)

      labeled_expand(
        eae_prk,
        @kem_suite_id,
        "shared_secret",
        encapsulated_key <> recipient_public_key,
        @key_size
      )
    end
  end

  defp hpke_key_schedule(shared_secret, info) do
    psk_id_hash = labeled_extract(<<>>, @hpke_suite_id, "psk_id_hash", <<>>)
    info_hash = labeled_extract(<<>>, @hpke_suite_id, "info_hash", info)
    key_schedule_context = <<0>> <> psk_id_hash <> info_hash
    secret = labeled_extract(shared_secret, @hpke_suite_id, "secret", <<>>)

    with {:ok, key} <-
           labeled_expand(secret, @hpke_suite_id, "key", key_schedule_context, @key_size),
         {:ok, nonce} <-
           labeled_expand(secret, @hpke_suite_id, "base_nonce", key_schedule_context, @nonce_size) do
      {:ok, key, nonce}
    end
  end

  defp labeled_extract(salt, suite_id, label, input_key_material) do
    hkdf_extract(salt, @hpke_version <> suite_id <> label <> input_key_material)
  end

  defp labeled_expand(prk, suite_id, label, info, length) do
    hkdf_expand(prk, <<length::16>> <> @hpke_version <> suite_id <> label <> info, length)
  end

  defp hkdf_extract(salt, input_key_material) do
    :crypto.mac(:hmac, :sha256, salt, input_key_material)
  end

  defp hkdf_expand(prk, info, length) when length <= 255 * @key_size do
    block_count = div(length + @key_size - 1, @key_size)

    {output, _previous} =
      Enum.reduce(1..block_count, {<<>>, <<>>}, fn index, {output, previous} ->
        block = :crypto.mac(:hmac, :sha256, prk, previous <> info <> <<index>>)
        {output <> block, block}
      end)

    {:ok, binary_part(output, 0, length)}
  end

  defp hkdf_expand(_prk, _info, length), do: {:error, {:hkdf_length_too_large, length}}

  defp envelope_aad(recipient_entries, context) do
    recipient_block = IO.iodata_to_binary(recipient_entries)

    <<@magic, @format_version, @kem_id::16, @kdf_id::16, @aead_id::16,
      length(recipient_entries)::16, recipient_block::binary, byte_size(context)::32,
      context::binary>>
  end

  defp decode_envelope(%{
         @marker => version,
         "kem" => @kem_id,
         "kdf" => @kdf_id,
         "aead" => @aead_id,
         "recipients" => encoded_entries,
         "nonce" => encoded_nonce,
         "ciphertext" => encoded_ciphertext,
         "tag" => encoded_tag
       }) do
    cond do
      version != @format_version ->
        {:error, {:unsupported_encryption_format, version}}

      not is_list(encoded_entries) or encoded_entries == [] ->
        {:error, :invalid_envelope}

      length(encoded_entries) > @max_recipients ->
        {:error, :too_many_recipients}

      true ->
        with {:ok, recipient_entries} <- decode_recipient_entries(encoded_entries),
             {:ok, nonce} <- decode64(encoded_nonce, @nonce_size),
             {:ok, ciphertext} <- decode64(encoded_ciphertext),
             {:ok, tag} <- decode64(encoded_tag, @tag_size) do
          {:ok, recipient_entries, nonce, ciphertext, tag}
        else
          :error -> {:error, :invalid_envelope}
          {:error, _reason} = error -> error
        end
    end
  end

  defp decode_envelope(%{@marker => version}) when version != @format_version,
    do: {:error, {:unsupported_encryption_format, version}}

  defp decode_envelope(_envelope), do: {:error, :invalid_envelope}

  defp decode_recipient_entries(encoded_entries) do
    encoded_entries
    |> Enum.reduce_while({:ok, []}, fn encoded, {:ok, entries} ->
      case decode64(encoded, @recipient_entry_size) do
        {:ok, entry} -> {:cont, {:ok, [entry | entries]}}
        :error -> {:halt, {:error, :invalid_envelope}}
      end
    end)
    |> case do
      {:ok, entries} -> {:ok, Enum.reverse(entries)}
      {:error, _reason} = error -> error
    end
  end

  defp rescued_reason(error) do
    if Map.has_key?(error, :original) do
      error.original
    else
      Exception.message(error)
    end
  end

  defp encode64(value), do: Base.url_encode64(value, padding: false)

  defp decode64(value) when is_binary(value), do: Base.url_decode64(value, padding: false)
  defp decode64(_value), do: :error

  defp decode64(value, expected_size) do
    case decode64(value) do
      {:ok, decoded} when byte_size(decoded) == expected_size -> {:ok, decoded}
      _other -> :error
    end
  end
end
