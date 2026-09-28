# Revalidate JWT expiry for long-lived WriteStream sessions

`WriteStream` now reauthenticates each event and terminates idle sessions at the verified JWT effective expiry (`exp + ClockSkew`). Expired messages are rejected before ingest without changing the wire contract or persistence schema.
