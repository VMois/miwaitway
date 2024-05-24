from gtfs_realtime_ingestor.ingest_vehicle_positions import build_waf_functions


def test_save_load_waf(tmp_path):
    data_path = tmp_path / "positions.csv"
    chunks_path = tmp_path / "chunks.txt"
    hash_path = tmp_path / "hash.txt"
    load_from_waf, save_to_waf = build_waf_functions(
        data_path, chunks_path, hash_path, 50
    )

    items = [{"id": 123}, {"id": 456}]
    chunks = 3
    hash = "abcd1234"

    save_to_waf(items, chunks, hash)

    assert data_path.exists()
    assert chunks_path.exists()
    assert hash_path.exists()

    loaded_items, loaded_chunks, loaded_hash = load_from_waf()

    assert items == loaded_items
    assert chunks == loaded_chunks
    assert hash == loaded_hash


def test_save_load_waf_empty(tmp_path):
    data_path = tmp_path / "positions.csv"
    chunks_path = tmp_path / "chunks.txt"
    hash_path = tmp_path / "hash.txt"
    load_from_waf, save_to_waf = build_waf_functions(
        data_path, chunks_path, hash_path, 50
    )

    items = []
    chunks = 3
    hash = "abcd1234"

    save_to_waf(items, chunks, hash)

    assert data_path.exists()
    assert chunks_path.exists()
    assert hash_path.exists()

    loaded_items, loaded_chunks, loaded_hash = load_from_waf()

    assert items == loaded_items
    assert chunks == loaded_chunks
    assert hash == loaded_hash
