-- D3 replaces the D1 pairing-code scaffold with credential-signed
-- RuntimeChannel HELLO. The table may exist on databases that replayed the
-- historical D1 migrations; it is no longer part of the live schema.
DROP TABLE IF EXISTS runtime_pairings;
