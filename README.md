# Simplex consensus for Avalanche

## Introduction

### Why Simplex?

The scientific literature is full of different consensus protocols,
and each has its own strengths and weaknesses.

Picking Simplex [1] as a consensus protocol of choice for Subnet only Validators has the following benefits:

- There is no view change sub-protocol, making it simple to implement.
- It has been peer-reviewed by the academic community (TCC 2023).
- Its censorship-resistance relies on leader rotation, unlike timeouts which may suffer from false positives.


The main counter-argument against choosing Simplex is that a single block proposer (leader) consensus protocol
has limited throughput compared to a protocol that shards the transactions space
across different nodes and proposes batches in parallel.

While the argument is correct, a fast consensus protocol isn't enough to guarantee a high end to end throughput for a blockchain.
To fully utilize parallel block proposers, the VM should also support distributed transaction processing.

As Avalanche's "high throughput" VM of choice is the HyperSDK, the techniques [it plans to employ](https://hackmd.io/@patrickogrady/rys8mdl5p#Integrating-Vryx-with-the-HyperSDK-High-Level-Sketch)
entail totally ordering only certificates of availability of chunks of transactions,
which are of small size.

Since the design of the HyperSDK does not require a high throughput consensus protocol,
there is no need to invest development time in a parallel block proposer consensus protocol.



### Node membership: PoS vs PoA setting

Simplex is suitable for both the Proof of Stake and the Proof of Authority settings.
For the sake of succinctness and simplicity, this document uses Proof of Authority (PoA) terminology.

A PoA setting can be thought of a PoS setting where all nodes have precisely the same stake,
making each node have a "single vote". Similarly, a PoS setting can be thought of a PoA setting
where the `N` nodes with the highest stake get to vote, and each node has a number of votes that is 
proportional to the stake of the node with the lowest stake among the `N` nodes.


## Protocol high-level description as per the Simplex paper

### Node membership:

The protocol assumes a static set of `n` nodes, out of which up to and not including a third, are faulty.

A quorum of nodes is defined as the smallest set such that two such sets of nodes intersect in at least one correct node. 
If `n=3f+1` where `f` is the upper bound on faulty nodes, then a quorum is any set of nodes of size `2f+1`.
Each node has a private key used for signing.
The corresponding public key used for verifying signatures is known to all other nodes.

### Rounds and roles:

A node progresses in monotonically increasing and successive rounds.
Each round has a (different) leader node designated to propose blocks and disseminate them to the rest of the nodes.
Nodes only respond to the first block they see from the leader of that round.
Once a leader node proposes a block, it participates in the remaining steps of the round as if it was a non-leader node.
Except from the step in which the leader broadcasts a block, every other step involves a node broadcasting a signed message. 
All nodes can verify whether a node indeed signed the message or not.

There exists a time period `T` that is a system-wide parameter and is used by the protocol.

For an  arbitrary length string `x`, we denote `H(x)` to be the hash of `x`,
where `H` is a collision resistant hash function with an output size of at least 32 bytes.

The flow of the protocol is as follows:

1. At round `i`, a leader node builds a block `b` and sends it to the rest of the nodes.
2. Each node broadcasts a vote `<vote, i, H(b)>` in favor of the block.
3. For each node, one of the two mutually exclusive events happen:
   1. If the node collects within time `T` a quorum of votes on `b` of the form `<vote, i, H(b)>` or for a quorum of votes on an empty block of the form `<vote, i, ⊥>`, the node then moves to participate in the next round `i+1`.
   2. Else, the node does not collect a quorum of votes within time `T`, and it then broadcasts a vote for an empty block `<vote, i, ⊥>` and waits to either collect a quorum of votes of the form `<vote, i, ⊥>` or `<vote, i, H(b)>` after which it moves to the next round `i+1`.
4. Upon collecting a quorum of votes  of the form `<vote, i, ⊥>` or `<vote, i, H(b)>`, the node broadcasts it before moving to round `i+1`.
5. Starting from round `i+1`, each node that did not vote for `<vote, i, ⊥>` (due to a timeout) or collect a quorum of votes on `<vote, i, ⊥>` broadcasts a finalization message `<finalize, i, H(b)>`.
6. Each node that collects a quorum of finalization messages considers the block `b` as finalized, and can deliver it to the application.

In our adaptation of Simplex, a node might also vote for the empty block if the application that uses it, considers
the leader to be faulty. An example in which the application may consider the leader is faulty, is if the leader hasn't proposed
a block, while the memory pool contains transactions.

It is up to the application to ensure that transactions should arrive to the leader in a timely manner.
A straight-forward way of ensuring this, is having nodes gossip transactions among each other.

If the application disseminates transactions via gossip, then correct nodes successfully notarize an empty block if and only if the leader is faulty: 

1. If the leader is correct, the empty block is not notarized:
   1. If the transaction is sent by a client to at least $f+1$ nodes, it will be disseminated to all correct nodes, so an empty block will be voted on by at most $f$ nodes.
   2. Else, the transaction is sent by a client at most $f$ nodes, so at most $f$ nodes will vote on the empty block, which is insufficient to notarize it.
2. If the leader is faulty and a transaction reached a correct node, then it will be gossipped to at least a quorum of correct nodes, and an empty vote will be notarized for that round. 


Similarly, the application may choose to monitor the connectivity to the leader node and make Simplex vote for the empty block
if it deems the leader as disconnected.

## Reconfiguring Simplex

The Simplex paper assumes a static set of nodes. 
However, validator nodes of a blockchain network may be added or removed while the protocol totally orders transactions. 
The act of adding or removing a validator node from the members of a consensus protocol is called reconfiguration.

When Simplex finalizes a block that causes the node membership to change, we say that block contains a reconfiguration event.

A reconfiguration event can be either a transaction that changes the node membership in the chain governed by the chain itself, 
or it can even be a proof originating from a chain not governed by the bespoken chain, which simply attests the new 
set of nodes running the Simplex consensus protocol. 

The only requirements for Simplex are:

1. All nodes participating in consensus must see the reconfiguration event in the same round.
2. The change must be atomic and not depend on anything but the blockchain (no reliance on external API calls).

Reconfiguring a Simplex protocol while it totally orders transactions poses several challenges:

1. In Simplex, node `p` may collect a quorum of finalization messages at a different round than node `q` 
(for example, block `b` was notarized by a quorum of nodes at round `i` but only node `p` 
collected a quorum of finalization votes in round `i+1`, and the rest voted for the empty block in round `i+1`
and then collected the finalization votes in round `i+2`). 
It is therefore not safe to apply the new node membership whenever a node collects a quorum of finalization votes, 
as nodes at the same round may end up having different opinions of which nodes are eligible to be part of a quorum.
2. In Simplex, blocks are proposed by alternating nodes.
It may be the case that node `p` proposes a block in round `i` which contains a transaction which removes node `q`
from the membership set, and the next node to propose a block is node `q`, in round `i+1`.
If `q` proposes a block in round `i+1`, and it is notarized before the block in round `i` is finalized, 
what is to become of the block proposed by `q` and other blocks built and notarized on top of it? 
Block `q` should not have been notarized because it was proposed by a node that was removed in round `i`,
and therefore conceptually, all transactions in successive blocks need to be returned into the mem-pool.

In order to be able to dynamically reconfigure the membership set of the Simplex protocol, two techniques will be employed:


* `Epochs`: Each round will be associated with an epoch. An epoch starts either at genesis or in the round successive to a round containing a reconfiguration event. An epoch ends when a block containing a reconfiguration event is finalized.

  The epoch number is always the sequence number of the block that caused the epoch to end. Meaning, if a block with a sequence of `i` in epoch `e` contained a reconfiguration event, the next epoch would be epoch number `i` regardless of `e`.

* `Parallel instances`: Once a node finalizes a block which contains a reconfiguration event, it starts a fresh new instance of Simplex for the new epoch, but still retains for a while the previous instance of the previous epoch, to respond to messages of straggler nodes in the previous epoch that haven’t finalized the last block of that epoch.

Given an old block `b` in epoch `e` and observing a block `b’` created in epoch `e+k` along with a set of finalization messages, it seems impossible to determine if `b’` is authentic or not, as there were reconfiguration events since the block `b` in epoch `e` was finalized. Therefore, the set of finalization messages on `b'` cannot be authenticated, as it’s not even clear how many nodes are there in epoch `e+k` in the first place.

Fortunately, there is a way to check whether block `b’` is authentic or not: Let `{e, e+i, …, e+j, e+k}` be the series of epochs from block `b` to block `b’`, and let {`b, d, …, d’, b’`} be blocks where the sequence number of `d` and `d’` are `e+i` and `e+j` respectively. To be convinced of the authenticity of block `b’` one can just fetch the series of blocks {`b, d, …, d’, b’`} and validate them one by one. Since each block encodes its epoch number, and epoch numbers are block sequences that contain reconfiguration events, 
then after validating the aforementioned series of blocks we are guaranteed whether `b’` is authentic or not.
Hereafter we call the series of blocks  {`b, d, …, d’, b’`}  an *Epoch Change Series* (ECS).

In practice, reconfiguration would work as follows:

1. Once a node finalizes a block `b` containing a reconfiguration event in round `i` at epoch `e`, it signals its current instance of Simplex to stop voting for non-empty blocks, and to refuse finalizing any descendant block of `b` to ensure `b` is the last finalized block in epoch `e`.
2. The node then spawns a new Simplex instance for epoch `e’>e`.
3. Once the node finalizes two blocks in epoch `e’`, it terminates the Simplex instance of epoch `e`. At this point, at least a quorum of nodes started epoch `e’`, and finalized at least one block in epoch `e’`. 
Therefore, at most `f` nodes are in the previous epoch `e`, so there is no need for the corresponding Simplex instance.

### Structuring the blockchain:

Unlike the official Simplex paper [1] where empty blocks end up as part of the blockchain, In our adaptation of it, the blockchain consists only of finalized blocks, and blocks are numbered consecutively but the round numbers in which these blocks were proposed, are not consecutive. Each block contains the corresponding protocol metadata for the round in which it was proposed.

![protocol metadata](docs/figures/blocks.png)


As depicted above, in case of an empty block (in white) the protocol metadata for the round is the round and epoch numbers. For regular blocks, the block sequence and the previous block hash are also part of the protocol metadata.
By looking at the sequence numbers and the round numbers of the two data blocks, it can be deduced that round 120 corresponds to an empty block.

### Storage of blocks and protocol state

Simplex will assume an abstract and pluggable block storage with two basic operations of retrieval of a block by its round number, and indexing a block by its round number.

```go
type Storage interface {  
   Retrieve(seq uint64) Block  
   Index(seq uint64, block Block)   
}
```

Where `Block` is defined as:

```go
type Block struct {
	InnerBlock SimplexBlock
	Finalization []SignedFinalization
}
```

The `Finalization` message is defined later on, and `SimplexBlock` is defined as follows:

```go
type SimplexBlock interface {
    // Metadata is the consensus specific metadata for the block
	Metadata() Metadata
	
	// Bytes returns a byte encoding of the block
	Bytes() []byte
}
```

Where `Metadata` is defined as:

```go
type Metadata struct {
    // Digest returns a collision resistant short representation of the block's bytes
    Digest []byte
    // Epoch returns the epoch in which the block was proposed
    Epoch uint64
    // Round returns the round number in which the block was proposed. 
    // Can also be an empty block.
    Round uint64
    // Seq is the order of the block among all blocks in the blockchain.
    // Cannot be an empty block.
    Seq uint64
    // Prev returns the digest of the previous data block
    Prev []byte
}
```

### Persisting protocol state to disk

Besides long term persistent storage, Simplex will also utilize a Write-Ahead-Log (WAL).   
A WAL is used to write intermediate steps in the consensus protocol’s operation.  
It’s needed for preserving consistency in the presence of crashes. Essentially, each node uses the WAL to save its current step in the protocol execution before it moves to the next step. If the node crashes, it uses the WAL to find at which step in the protocol it crashed and knows how to resume its operation from that step.  
The WAL will be implemented as an append-only file which will be pruned only upon a finalization of a block. The WAL interface will be as follows:

```go
type WriteAheadLog interface {  
   Append(Record)  
   ReadAll() []Record  
}
```


Where Record is defined as:

```protobuf
Record {  
   size uint32  
   type uint32  
   payload bytes  
   checksum bytes   
}
```


The type corresponds to which message is recorded in the record, and each type corresponds to a serialization of one of the following messages:

- The proposed message is written to the WAL once a node receives a proposal from the leader. It contains the block in its raw form, without finalizations.

```protobuf
Proposed {  
  block bytes  
}
```

- A vote message is sent by a node right after it persists the Proposed message to the WAL. The signature is over the ASN1 encoding of a Vote message.

<table>
<tr>
</tr>
<tr>
<td>

```protobuf
Vote {  
   version uint32  
   digest bytes  
   digest_algorithm uint32  
   seq uint64  
   round uint64  
   epoch uint64  
   prev bytes
}
```
</td>
<td>

```protobuf
SignedVote {
    vote Vote
    signature_algorithm uint32
    signer bytes
    signature bytes
}
    
    
    
```

</td>
</tr>
</table>

- Once a node collects a quorum of vote messages from distinct signers, it persists the following message to the WAL:

```protobuf
Notarization {  
  repeated votes SignedVote  
}
```


- Of course, it might be that it’s not possible to collect a quorum of Vote messages, and in that case the node times out until it collects a quorum of EmptyVote messages:


<table>
<tr>
<td>

```protobuf
EmptyVote {  
   version uint32  
   round uint64  
   epoch uint64  

}
```

</td>
<td>

```protobuf
SignedEmptyVote {  
   empty_vote EmptyVote  
   signature_algorithm uint16  
   signer bytes  
   signature bytes  
}
```

</td>
</tr>
</table>

- The node then writes the EmptyNotarization message into the WAL.


```protobuf
EmptyNotarization {  
   repeated empty_votes SignedEmptyVote  
}
```


- The last message that is related to totally ordering blocks, is the Finalization message:

<table>
<tr>
<td>

```protobuf
Finalization {  
   version uint16  
   digest bytes  
   digest_algorithm uint32  
   seq uint64  
   round uint64  
   epoch uint64  
   prev bytes  
}
```

</td>
<td>

```protobuf
SignedFinalization {  
   finalization Finalization  
   signature_algorithm uint16  
   signer bytes  
   signature bytes  
}



```

</td>
</tr>
</table>


Unlike the rest of the messages, a finalization message isn’t written into the WAL, but instead written into the storage atomically with the block.

In case the signature algorithm allows aggregation of signatures, we define the following messages:

<table>
<tr>
<td>

```protobuf
AggregatedSignedVote {  
    vote Vote  
    signature_algorithm uint16  
    signers repeated bytes
    signature bytes  
}
```

</td>
<td>

```protobuf
AggregatedSignedEmptyVote {  
    empty_vote EmptyVote  
    signature_algorithm uint32
    signers repeated bytes  
    signature bytes  
}
```

</td>

<td>

```protobuf
AggregatedSignedFinalization {  
    finalization Finalization  
    signature_algorithm uint16  
    signers repeated bytes  
    signature bytes  
}
```

</td>
</tr>
</table>


Two useful facts can be deduced from the structure of the messages written to the WAL:

1. The WAL of a node is not unique to that node, and two nodes in the same round may have the same WAL content.
2. Nodes can verify each other's WAL content. 

Combining the two above facts leads to a conclusion: Nodes can safely replicate the WAL from each other.

### Simplex and synchronization of disconnected nodes

In a real internet wide deployment, nodes may experience message loss due to plenty of reasons.  A consensus protocol that relies on messages being broadcast once, needs to be able to withstand sudden and unexpected message loss or know how to recover when they occur.  Otherwise, a node that missed a message or two, may find itself stuck in a round as the rest of the nodes advance to higher rounds. While consistency is not impacted, degradation of liveness of a consensus protocol can be severely detrimental to end user experience.

A node that has been disconnected or just missed messages can synchronize with other nodes.  
The synchronization mechanism is divided into two independent aspects:

1. Detection of the straggler node that it needs to synchronize
2. The straggler node synchronizing

In order to detect that a node is straggling behind, nodes periodically send each other a message containing the current epoch and round number.

````protobuf
Heartbeat {
  epoch uint64
  round uint64
}
````

A node considers itself behind if a majority of nodes are in a higher round than it.

If a node discovers it is behind, there are two mutually exclusive scenarios:

1. The node is behind in the round number, but not in the epoch number.
2. The node is behind also in the epoch number.

If the node is behind just in the round number, it reaches to the nodes it knows and fetches missing blocks (with finalizations) as well as notarizations and empty notarizations.

However, if the node is behind also in the epoch number, it first reaches the nodes it knows in order to fetch an ECS, in order to discover the latest set of members.   
Once it has established the latest membership of nodes it needs to interact with, it proceeds with synchronizing the blocks, notarizations and empty notarizations.

The blocks and (empty) notarizations are replicated subject to the following rules:

1. A notarization in round `i` isn’t fetched if there exists a block in round `j > i` (replicating nodes prefer to replicate blocks over notarizations).
2. Blocks (along with the corresponding finalizations) are fetched and written to the storage in-order.
3. If the last message in the WAL is about round `i` and a block of round `j > i` is fetched, the WAL is pruned.
4. If a notarization of a regular block is fetched from a remote node, then the block corresponding to the notarization must be fetched as well.
5. If an empty notarization is fetched from a remote node, then the block for that round needs not to be fetched.
6. A node that replicated a notarization, writes it to the WAL in the same manner it would have written it during its normal operation.

In order for a node to synchronize the WAL, we define the following messages:

<table>
<tr>
<td>

```protobuf
NotarizationRequest {
  seq uint64
}
```

</td>
<td>

```protobuf
EmptyNotarizationRequest {
  seq uint64
}
```

</td>
<td>

```protobuf
BlockRequest {
  seq uint64
}
```

</td>
</tr>
</table>

<table>
<tr>
<td>

```protobuf
NotarizationResponse {
  votes repeated SignedVote
}
```

</td>
<td>

```protobuf
EmptyNotarizationResponse {
  votes repeated SignedEmptyVote
}
```

</td>
<td>

```protobuf
BlockResponse {
  block Block
}
```

</td>
</tr>
</table>

## Simplex API

In order for the consensus protocol to be part of an application, such as avalanchego, 
Simplex will both depend on APIs from the application, and also give APIs for the application to use it.

Examples of APIs that Simplex would expect from the application, 
are APIs for sending and signing messages, an API that would be triggerred once a block has been finalized,
and an API to build a block.

An example of an API that Simplex would expose to the application, as an API to receive messages from the network.

In order to make the API compact, an all-purpose `Message` struct would be defined:

```go
type Message struct {
	Type MsgType
	// One of:
	*V SignedVote
	*E SignedEmptyVote
	*F SignedFinalization
	*N NotarizationRequest
	*E EmptyNotarizationRequest
	*B BlockRequest
	*NR NotarizationResponse
	*ER EmptyNotarizationResponse
	*BR BlockResponse
}
```

It is the responsibility of the application to properly handle authentication, marshalling and wire protocol,
and to construct the `Message` properly.

Compared to the snowman consensus, the Simplex API would be at the engine level. 
The reason for that it would be possible to integrate Simplex into a toy application for early testing,
and also in order to be able to structure tests to run an entire network of nodes.

### The Simplex engine API to the application:

```go
type Consensus interface {
    // AdvanceTime hints the engine that the given amount of time has passed.
    AdvanceTime(time.Duration)
    
    // HandleMessage notifies the engine about a reception of a message.
    HandleMessage(Message)
	
    // Suspect conveys the application suspects a node with the given ID is faulty.
    Suspect(ID bytes)

}

```

### The application API to the Simplex engine consists of several objects:

The two most important APIs that an application exposes to Simplex, 
are an API to build blocks and as mentioned before, an API to store and retrieve them:

<table>
<tr>
<td>

```go
type BlockBuilder interface {
    BuildBlock() SimplexBlock	
	
}
```

</td>
<td>

```go
type Storage interface {
    Retrieve(seq uint64) Block
    Index(seq uint64, block Block)
}
```

</td>
</tr>
</table>


Whenever a Simplex instance recognizes it is its turn to propose a block, it calls
into the application in order to build a block. Similarly, when it has collected enough finalizations for a block,
it passes the block and the finalizations to the application layer, which in turn, is responsible
not only for indexing the block, but also removing the transactions of the block from the memory pool.

In order for signatures on notarizations or finalizations to be verified, we define the following verification object:

```go
type Verifier interface {

    // VerifySignature verifies the signature of the given signer on the given digest.
    VerifySignature(signer bytes, digest bytes, signature bytes) error

    // SetEpochChange sets the epoch to correspond with the epoch
    // that is the result of committing this block.
    // If the block doesn't cause an epoch change, this is a no-op.
    SetEpochChange(SimplexBlock) error
	
}
```
A `Verifier` can be configured to verify the next epoch by consuming the last block of an epoch.

In order to detect whether a commit of a block would cause the current epoch to end,
we define the following API:

```go
type EpochEnder interface {
	// EndsEpoch returns whether the given block ends the epoch.
	EndsEpoch(SimplexBlock) bool
}

```


In order to send messages to the members of the network, a communication object
which also can be configured on an epoch basis, is defined:

```go
type Communication interface {
	
    // ListNodes returns all nodes known to the application.
    ListNodes() []bytes
	
    // SendMessage sends a message to the given destination node
    SendMessage(msg Message, destination bytes)
    
    // Broadcast broadcasts the given message to all nodes 
    Broadcast(msg Message)

    // SetEpochChange sets the epoch to correspond with the epoch
    // that is the result of committing this block.
    // If the block doesn't cause an epoch change, this is a no-op.
    SetEpochChange(SimplexBlock) error
	
}
```

[1] https://eprint.iacr.org/2023/463


## Acknowledgements

Thanks to Stephen Buttolph for invaluable feedback on this specification.