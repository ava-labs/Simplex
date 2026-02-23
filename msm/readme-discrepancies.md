# Discrepancies between README.md and Code

The following discrepancies were found by comparing the MSM README specification against the implementation in the Go code.

## 1. Sealing block `sealing_block_seq` -- README contradicts code and itself

**README (line 246):**
> "A sealing block is identified by having the `block_validation_descriptor` defined and `sealing_block_seq` set to 0."

**Code (`createSealingBlock`, msm.go):**
```go
simplexEpochInfo.SealingBlockSeq = md.Seq + 1  // the sealing block's own sequence, NOT 0
```

The code sets `SealingBlockSeq` to the sealing block's own sequence number (always > 0). The README says it's 0.

The README also contradicts itself -- line 248 says Telocks have `sealing_block_seq` set to the sealing block's sequence, and line 249 says Telocks are identified by `sealing_block_seq > 0`. If the sealing block itself had `sealing_block_seq == 0`, Telocks would inherit 0, which contradicts the `> 0` identification.

## 2. Signature message -- `PChainReferenceHeight` vs `next_p_chain_reference_height`

**README (line 211):**
> "The signature is over the `info` field of the `AuxiliaryInfo` and the **`next_p_chain_reference_height`** fields"

**Code (`verifySignature`, verification.go):**
```go
pChainHeightBuff := pChainReferenceHeightAsBytes(prev)  // prev.PChainReferenceHeight
bb.Write(pChainHeightBuff)
bb.Write(auxinfo.Info)
```

The code signs/verifies over `PChainReferenceHeight` (current epoch's height), **not** `NextPChainReferenceHeight`. Either the README or the code is wrong. If the README is correct, the code has a security vulnerability -- the signature doesn't bind to the target transition height.

## 3. Missing enforcement of `next_p_chain_reference_height > p_chain_reference_height`

**README (line 140):**
> "`next_p_chain_reference_height > p_chain_reference_height` or `next_p_chain_reference_height == 0`"

**Code (`verifyNextPChainHeightNormal`, verification.go):** Does not check that `NextPChainReferenceHeight > PChainReferenceHeight`. A proposer could set `NextPChainReferenceHeight` to a value less than `PChainReferenceHeight` if there happens to be a different validator set at that lower height.

## 4. Missing enforcement that validator sets must differ when `next_p_chain_reference_height > 0`

**README (line 142):**
> "If `next_p_chain_reference_height > 0`, the validator set derived from the P-chain height is **different** from the validator set derived by `p_chain_reference_height`"

**Code (verification.go):**
```go
if currentValidatorSet.Compare(newValidatorSet) {
    return nil  // same validator set -- accepted without error
}
```

When validator sets are identical, the block is accepted even with `NextPChainReferenceHeight > 0`. This allows spurious epoch transitions with no actual validator set change.

## 5. `prev_sealing_block_hash` for the first epoch

**README (line 88-89):**
> "If there is no previous epoch (i.e., the current epoch is the first ever epoch), then it is nil."

**Code (`createSealingBlock`, msm.go):** For epoch 1 (the first epoch), `PrevSealingBlockHash` is set to the hash of the first simplex block, not nil/zero.

## 6. `NodeBLSMapping` missing `Weight` field in README

**README (line 428-431):**
```proto
message NodeBLSMapping {
    bytes node_id = 1;
    bytes bls_key = 2;
}
```

**Code (encoding.go):** Has an additional `Weight uint64` field used for quorum calculations. The README omits it.

## 7. Block digest computation

**README (lines 401-411):** Describes the digest as the hash of a proto message `HashPreImage` with fields `h_i` (field 1) and `h_m` (field 2), which would include protobuf field tags and length prefixes.

**Code (`Digest()`, msm.go):**
```go
combined := make([]byte, 64)
copy(combined[:32], blockDigest[:])
copy(combined[32:], mdDigest[:])
return sha256.Sum256(combined)
```

The code does a raw 64-byte concatenation, not a protobuf-encoded message. An implementation following the README would produce different hashes.

## Summary

| # | Discrepancy | Impact |
|---|------------|--------|
| 1 | `sealing_block_seq` value for sealing blocks | README self-contradictory |
| 2 | Signature over wrong P-chain height field | Potentially high -- code or README is wrong |
| 3 | No `NextPChainReferenceHeight > PChainReferenceHeight` check | Medium -- allows downward transitions |
| 4 | No enforcement that validator sets differ | Low -- allows spurious transitions |
| 5 | `prev_sealing_block_hash` non-nil for first epoch | Documentation mismatch |
| 6 | Missing `Weight` field in README | Documentation incomplete |
| 7 | Digest computation method | Interoperability risk |