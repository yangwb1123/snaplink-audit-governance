package security

// MinConfiguredSecretBytes is the minimum number of UTF-8 encoded bytes
// required for signing and encryption secrets at the service configuration
// boundary outside development mode. Direct security package callers retain
// compatibility with shorter keys.
const MinConfiguredSecretBytes = 32
