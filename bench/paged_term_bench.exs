# D4 benchmark: page size + snapshot threshold defaults for LTX segment
# persistence. Run with:
#
#     mix run bench/paged_term_bench.exs
#
# Measures, per state size / page size / mutation pattern:
#   - encode time (deterministic term_to_binary + paging + checksums)
#   - diff time and changed-page count
#   - actual LTX delta segment size (real Encoder output, literals-only LZ4)
#   - full snapshot segment size (the "write it all" baseline)

alias DurableServer.LTX
alias DurableServer.LTX.{Encoder, PagedTerm}

defmodule Bench do
  import Bitwise

  def state(bytes) do
    # A map of 32 binary values — representative of accumulated server state.
    value_size = max(div(bytes, 32), 8)
    Map.new(1..32, fn i -> {:"field_#{String.pad_leading("#{i}", 2, "0")}", pattern(i, value_size)} end)
  end

  def pattern(seed, size), do: :binary.copy(<<rem(seed * 37, 251)>>, size)

  def mutate(state, :point) do
    # Same-length in-place change to one middle field.
    %{state | field_16: pattern(99, byte_size(state.field_16))}
  end

  def mutate(state, :append) do
    # Grow one late-sorting field by a small amount (append-heavy workload).
    Map.put(state, :field_32, state.field_32 <> pattern(7, 64))
  end

  def mutate(state, :shift) do
    # Length change in an early field — shifts every later byte (worst case).
    %{state | field_01: state.field_01 <> <<1>>}
  end

  def segment_size(pages, page_size, commit, snapshot?) do
    header = %LTX.Header{
      page_size: page_size,
      commit: commit,
      min_txid: if(snapshot?, do: 1, else: 2),
      max_txid: if(snapshot?, do: 1, else: 2),
      pre_apply_checksum: if(snapshot?, do: 0, else: LTX.checksum_flag() ||| 1)
    }

    {:ok, enc} = Encoder.new(header)

    enc =
      Enum.reduce(pages, enc, fn {pgno, data}, enc ->
        {:ok, enc} = Encoder.encode_page(enc, pgno, data)
        enc
      end)

    {:ok, iodata} = Encoder.finish(enc, LTX.checksum_flag() ||| 2)
    IO.iodata_to_binary(iodata) |> byte_size()
  end

  def time(fun) do
    {micros, result} = :timer.tc(fun)
    {micros, result}
  end

  def kb(bytes), do: :erlang.float_to_binary(bytes / 1024, decimals: 1) <> "K"

  def run do
    IO.puts(
      String.pad_trailing("state", 8) <>
        String.pad_trailing("pgsz", 6) <>
        String.pad_trailing("mutation", 9) <>
        String.pad_trailing("pages", 7) <>
        String.pad_trailing("Δpages", 8) <>
        String.pad_trailing("Δseg", 9) <>
        String.pad_trailing("snap", 9) <>
        String.pad_trailing("Δ/snap", 8) <>
        String.pad_trailing("encµs", 8) <> "diffµs"
    )

    for state_bytes <- [1_024, 10_240, 102_400, 1_048_576, 10_485_760],
        page_size <- [512, 1_024, 4_096, 8_192] do
      base = state(state_bytes)
      {:ok, base_image} = PagedTerm.encode(base, page_size)

      snapshot_size =
        segment_size(Enum.sort(base_image.pages), page_size, base_image.commit, true)

      for mutation <- [:point, :append, :shift] do
        changed = mutate(base, mutation)

        {enc_us, {:ok, image}} =
          time(fn -> PagedTerm.encode(changed, page_size, base_image) end)

        {diff_us, delta} =
          time(fn -> PagedTerm.changed_pages(base_image.page_checksums, image) end)

        delta_size =
          if delta == [] do
            0
          else
            segment_size(delta, page_size, image.commit, false)
          end

        ratio = :erlang.float_to_binary(delta_size / snapshot_size, decimals: 3)

        IO.puts(
          String.pad_trailing(kb(state_bytes), 8) <>
            String.pad_trailing("#{page_size}", 6) <>
            String.pad_trailing("#{mutation}", 9) <>
            String.pad_trailing("#{image.commit}", 7) <>
            String.pad_trailing("#{length(delta)}", 8) <>
            String.pad_trailing(kb(delta_size), 9) <>
            String.pad_trailing(kb(snapshot_size), 9) <>
            String.pad_trailing(ratio, 8) <>
            String.pad_trailing("#{enc_us}", 8) <> "#{diff_us}"
        )
      end
    end
  end
end

Bench.run()
