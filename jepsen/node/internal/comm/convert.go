package comm

import (
	"context"
	"fmt"

	"github.com/ava-labs/simplex/common"
	pb "github.com/ava-labs/simplex/jepsen/node/proto"
)

// toProto converts a common.Message to its protobuf representation.
func toProto(msg *common.Message) (*pb.SimplexMessage, error) {
	switch {
	case msg.BlockMessage != nil:
		bm := msg.BlockMessage
		blockBytesRaw, err := blockBytesFromBlock(bm.Block)
		if err != nil {
			return nil, fmt.Errorf("toProto BlockMessage: %w", err)
		}
		return &pb.SimplexMessage{
			Payload: &pb.SimplexMessage_BlockMessage{
				BlockMessage: &pb.BlockMessage{
					BlockBytes: blockBytesRaw,
					Vote:       voteToProto(bm.Vote),
				},
			},
		}, nil

	case msg.VerifiedBlockMessage != nil:
		vbm := msg.VerifiedBlockMessage
		blockBytesRaw, err := vbm.VerifiedBlock.Bytes()
		if err != nil {
			return nil, fmt.Errorf("toProto VerifiedBlockMessage: %w", err)
		}
		return &pb.SimplexMessage{
			Payload: &pb.SimplexMessage_BlockMessage{
				BlockMessage: &pb.BlockMessage{
					BlockBytes: blockBytesRaw,
					Vote:       voteToProto(vbm.Vote),
				},
			},
		}, nil

	case msg.VoteMessage != nil:
		return &pb.SimplexMessage{
			Payload: &pb.SimplexMessage_VoteMessage{
				VoteMessage: voteToProto(*msg.VoteMessage),
			},
		}, nil

	case msg.EmptyVoteMessage != nil:
		ev := msg.EmptyVoteMessage
		return &pb.SimplexMessage{
			Payload: &pb.SimplexMessage_EmptyVoteMessage{
				EmptyVoteMessage: &pb.EmptyVote{
					Round:     ev.Vote.Round,
					Epoch:     ev.Vote.Epoch,
					Signature: sigToProto(ev.Signature),
				},
			},
		}, nil

	case msg.Notarization != nil:
		n := msg.Notarization
		return &pb.SimplexMessage{
			Payload: &pb.SimplexMessage_Notarization{
				Notarization: &pb.Notarization{
					BlockHeader: blockHeaderToProto(n.Vote.BlockHeader),
					Qc:          qcToProto(n.QC),
				},
			},
		}, nil

	case msg.EmptyNotarization != nil:
		en := msg.EmptyNotarization
		return &pb.SimplexMessage{
			Payload: &pb.SimplexMessage_EmptyNotarization{
				EmptyNotarization: &pb.EmptyNotarization{
					Round: en.Vote.Round,
					Epoch: en.Vote.Epoch,
					Qc:    qcToProto(en.QC),
				},
			},
		}, nil

	case msg.FinalizeVote != nil:
		fv := msg.FinalizeVote
		return &pb.SimplexMessage{
			Payload: &pb.SimplexMessage_FinalizeVote{
				FinalizeVote: &pb.FinalizeVote{
					BlockHeader: blockHeaderToProto(fv.Finalization.BlockHeader),
					Signature:   sigToProto(fv.Signature),
				},
			},
		}, nil

	case msg.Finalization != nil:
		f := msg.Finalization
		return &pb.SimplexMessage{
			Payload: &pb.SimplexMessage_Finalization{
				Finalization: &pb.Finalization{
					BlockHeader: blockHeaderToProto(f.Finalization.BlockHeader),
					Qc:          qcToProto(f.QC),
				},
			},
		}, nil

	case msg.ReplicationRequest != nil:
		rr := msg.ReplicationRequest
		return &pb.SimplexMessage{
			Payload: &pb.SimplexMessage_ReplicationRequest{
				ReplicationRequest: &pb.ReplicationRequest{
					Seqs:               rr.Seqs,
					Rounds:             rr.Rounds,
					LatestRound:        rr.LatestRound,
					LatestFinalizedSeq: rr.LatestFinalizedSeq,
				},
			},
		}, nil

	case msg.ReplicationResponse != nil:
		resp := msg.ReplicationResponse
		pbData := make([]*pb.QuorumRound, 0, len(resp.Data))
		for _, qr := range resp.Data {
			pbQR, err := quorumRoundToProto(&qr)
			if err != nil {
				return nil, err
			}
			pbData = append(pbData, pbQR)
		}
		pbMsg := &pb.ReplicationResponse{
			Data: pbData,
		}
		if resp.LatestRound != nil {
			pbQR, err := quorumRoundToProto(resp.LatestRound)
			if err != nil {
				return nil, err
			}
			pbMsg.LatestRound = pbQR
		}
		if resp.LatestSeq != nil {
			pbQR, err := quorumRoundToProto(resp.LatestSeq)
			if err != nil {
				return nil, err
			}
			pbMsg.LatestSeq = pbQR
		}
		return &pb.SimplexMessage{
			Payload: &pb.SimplexMessage_ReplicationResponse{
				ReplicationResponse: pbMsg,
			},
		}, nil

	case msg.BlockDigestRequest != nil:
		bdr := msg.BlockDigestRequest
		return &pb.SimplexMessage{
			Payload: &pb.SimplexMessage_BlockDigestRequest{
				BlockDigestRequest: &pb.BlockDigestRequest{
					Seq:    bdr.Seq,
					Digest: bdr.Digest[:],
				},
			},
		}, nil

	default:
		return nil, fmt.Errorf("toProto: unknown message type")
	}
}

// fromProto converts a protobuf SimplexMessage back to common.Message.
func fromProto(pbMsg *pb.SimplexMessage, bd common.BlockDeserializer, qd common.QCDeserializer) (*common.Message, error) {
	if pbMsg == nil {
		return nil, fmt.Errorf("fromProto: nil message")
	}

	msg := &common.Message{}

	switch p := pbMsg.Payload.(type) {
	case *pb.SimplexMessage_BlockMessage:
		bm := p.BlockMessage
		blk, err := bd.DeserializeBlock(context.Background(), bm.BlockBytes)
		if err != nil {
			return nil, fmt.Errorf("fromProto BlockMessage DeserializeBlock: %w", err)
		}
		vote, err := voteFromProto(bm.Vote)
		if err != nil {
			return nil, fmt.Errorf("fromProto BlockMessage vote: %w", err)
		}
		msg.BlockMessage = &common.BlockMessage{
			Block: blk,
			Vote:  vote,
		}

	case *pb.SimplexMessage_VoteMessage:
		vote, err := voteFromProto(p.VoteMessage)
		if err != nil {
			return nil, fmt.Errorf("fromProto VoteMessage: %w", err)
		}
		msg.VoteMessage = &vote

	case *pb.SimplexMessage_EmptyVoteMessage:
		ev := p.EmptyVoteMessage
		msg.EmptyVoteMessage = &common.EmptyVote{
			Vote: common.ToBeSignedEmptyVote{
				EmptyVoteMetadata: common.EmptyVoteMetadata{
					Round: ev.Round,
					Epoch: ev.Epoch,
				},
			},
			Signature: sigFromProto(ev.Signature),
		}

	case *pb.SimplexMessage_Notarization:
		n := p.Notarization
		qc, err := qd.DeserializeQuorumCertificate(n.Qc.Data)
		if err != nil {
			return nil, fmt.Errorf("fromProto Notarization QC: %w", err)
		}
		bh, err := blockHeaderFromProto(n.BlockHeader)
		if err != nil {
			return nil, err
		}
		msg.Notarization = &common.Notarization{
			Vote: common.ToBeSignedVote{BlockHeader: bh},
			QC:   qc,
		}

	case *pb.SimplexMessage_EmptyNotarization:
		en := p.EmptyNotarization
		qc, err := qd.DeserializeQuorumCertificate(en.Qc.Data)
		if err != nil {
			return nil, fmt.Errorf("fromProto EmptyNotarization QC: %w", err)
		}
		msg.EmptyNotarization = &common.EmptyNotarization{
			Vote: common.ToBeSignedEmptyVote{
				EmptyVoteMetadata: common.EmptyVoteMetadata{
					Round: en.Round,
					Epoch: en.Epoch,
				},
			},
			QC: qc,
		}

	case *pb.SimplexMessage_FinalizeVote:
		fv := p.FinalizeVote
		bh, err := blockHeaderFromProto(fv.BlockHeader)
		if err != nil {
			return nil, err
		}
		msg.FinalizeVote = &common.FinalizeVote{
			Finalization: common.ToBeSignedFinalization{BlockHeader: bh},
			Signature:    sigFromProto(fv.Signature),
		}

	case *pb.SimplexMessage_Finalization:
		f := p.Finalization
		bh, err := blockHeaderFromProto(f.BlockHeader)
		if err != nil {
			return nil, err
		}
		qc, err := qd.DeserializeQuorumCertificate(f.Qc.Data)
		if err != nil {
			return nil, fmt.Errorf("fromProto Finalization QC: %w", err)
		}
		msg.Finalization = &common.Finalization{
			Finalization: common.ToBeSignedFinalization{BlockHeader: bh},
			QC:           qc,
		}

	case *pb.SimplexMessage_ReplicationRequest:
		rr := p.ReplicationRequest
		msg.ReplicationRequest = &common.ReplicationRequest{
			Seqs:               rr.Seqs,
			Rounds:             rr.Rounds,
			LatestRound:        rr.LatestRound,
			LatestFinalizedSeq: rr.LatestFinalizedSeq,
		}

	case *pb.SimplexMessage_ReplicationResponse:
		resp := p.ReplicationResponse
		data := make([]common.QuorumRound, 0, len(resp.Data))
		for _, pbQR := range resp.Data {
			qr, err := quorumRoundFromProto(pbQR, bd, qd)
			if err != nil {
				return nil, err
			}
			data = append(data, qr)
		}
		rrMsg := &common.ReplicationResponse{Data: data}
		if resp.LatestRound != nil {
			qr, err := quorumRoundFromProto(resp.LatestRound, bd, qd)
			if err != nil {
				return nil, err
			}
			rrMsg.LatestRound = &qr
		}
		if resp.LatestSeq != nil {
			qr, err := quorumRoundFromProto(resp.LatestSeq, bd, qd)
			if err != nil {
				return nil, err
			}
			rrMsg.LatestSeq = &qr
		}
		msg.ReplicationResponse = rrMsg

	case *pb.SimplexMessage_BlockDigestRequest:
		bdr := p.BlockDigestRequest
		var digest common.Digest
		copy(digest[:], bdr.Digest)
		msg.BlockDigestRequest = &common.BlockDigestRequest{
			Seq:    bdr.Seq,
			Digest: digest,
		}

	default:
		return nil, fmt.Errorf("fromProto: unknown payload type %T", pbMsg.Payload)
	}

	return msg, nil
}

// Helper converters

func blockBytesFromBlock(blk common.Block) ([]byte, error) {
	// Block may implement VerifiedBlock with Bytes()
	if vb, ok := blk.(common.VerifiedBlock); ok {
		return vb.Bytes()
	}
	return nil, fmt.Errorf("block does not implement VerifiedBlock")
}

func metadataToProto(md common.ProtocolMetadata) *pb.ProtocolMetadata {
	return &pb.ProtocolMetadata{
		Version: uint32(md.Version),
		Epoch:   md.Epoch,
		Round:   md.Round,
		Seq:     md.Seq,
		Prev:    md.Prev[:],
	}
}

func metadataFromProto(p *pb.ProtocolMetadata) (common.ProtocolMetadata, error) {
	if p == nil {
		return common.ProtocolMetadata{}, fmt.Errorf("nil ProtocolMetadata")
	}
	md := common.ProtocolMetadata{
		Version: uint8(p.Version),
		Epoch:   p.Epoch,
		Round:   p.Round,
		Seq:     p.Seq,
	}
	copy(md.Prev[:], p.Prev)
	return md, nil
}

func blockHeaderToProto(bh common.BlockHeader) *pb.BlockHeader {
	return &pb.BlockHeader{
		Metadata: metadataToProto(bh.ProtocolMetadata),
		Digest:   bh.Digest[:],
	}
}

func blockHeaderFromProto(p *pb.BlockHeader) (common.BlockHeader, error) {
	if p == nil {
		return common.BlockHeader{}, fmt.Errorf("nil BlockHeader")
	}
	md, err := metadataFromProto(p.Metadata)
	if err != nil {
		return common.BlockHeader{}, err
	}
	bh := common.BlockHeader{ProtocolMetadata: md}
	copy(bh.Digest[:], p.Digest)
	return bh, nil
}

func sigToProto(sig common.Signature) *pb.Signature {
	return &pb.Signature{
		Signer: sig.Signer,
		Value:  sig.Value,
	}
}

func sigFromProto(p *pb.Signature) common.Signature {
	if p == nil {
		return common.Signature{}
	}
	return common.Signature{
		Signer: common.NodeID(p.Signer),
		Value:  p.Value,
	}
}

func qcToProto(qc common.QuorumCertificate) *pb.QuorumCertificate {
	if qc == nil {
		return &pb.QuorumCertificate{}
	}
	signerIDs := qc.Signers()
	signers := make([][]byte, len(signerIDs))
	for i, s := range signerIDs {
		signers[i] = s
	}
	return &pb.QuorumCertificate{
		Signers: signers,
		Data:    qc.Bytes(),
	}
}

func voteToProto(v common.Vote) *pb.Vote {
	return &pb.Vote{
		BlockHeader: blockHeaderToProto(v.Vote.BlockHeader),
		Signature:   sigToProto(v.Signature),
	}
}

func voteFromProto(p *pb.Vote) (common.Vote, error) {
	if p == nil {
		return common.Vote{}, fmt.Errorf("nil Vote")
	}
	bh, err := blockHeaderFromProto(p.BlockHeader)
	if err != nil {
		return common.Vote{}, err
	}
	return common.Vote{
		Vote:      common.ToBeSignedVote{BlockHeader: bh},
		Signature: sigFromProto(p.Signature),
	}, nil
}

func quorumRoundToProto(qr *common.QuorumRound) (*pb.QuorumRound, error) {
	pbQR := &pb.QuorumRound{}
	if qr.Block != nil {
		blockBytes, err := blockBytesFromBlock(qr.Block)
		if err != nil {
			return nil, fmt.Errorf("quorumRoundToProto: %w", err)
		}
		pbQR.BlockBytes = blockBytes
	}
	if qr.Notarization != nil {
		pbQR.Notarization = &pb.Notarization{
			BlockHeader: blockHeaderToProto(qr.Notarization.Vote.BlockHeader),
			Qc:          qcToProto(qr.Notarization.QC),
		}
	}
	if qr.Finalization != nil {
		pbQR.Finalization = &pb.Finalization{
			BlockHeader: blockHeaderToProto(qr.Finalization.Finalization.BlockHeader),
			Qc:          qcToProto(qr.Finalization.QC),
		}
	}
	if qr.EmptyNotarization != nil {
		en := qr.EmptyNotarization
		pbQR.EmptyNotarization = &pb.EmptyNotarization{
			Round: en.Vote.Round,
			Epoch: en.Vote.Epoch,
			Qc:    qcToProto(en.QC),
		}
	}
	return pbQR, nil
}

func quorumRoundFromProto(p *pb.QuorumRound, bd common.BlockDeserializer, qd common.QCDeserializer) (common.QuorumRound, error) {
	qr := common.QuorumRound{}

	if len(p.BlockBytes) > 0 {
		blk, err := bd.DeserializeBlock(context.Background(), p.BlockBytes)
		if err != nil {
			return qr, fmt.Errorf("quorumRoundFromProto DeserializeBlock: %w", err)
		}
		qr.Block = blk
	}
	if p.Notarization != nil {
		bh, err := blockHeaderFromProto(p.Notarization.BlockHeader)
		if err != nil {
			return qr, err
		}
		qc, err := qd.DeserializeQuorumCertificate(p.Notarization.Qc.Data)
		if err != nil {
			return qr, fmt.Errorf("quorumRoundFromProto Notarization QC: %w", err)
		}
		qr.Notarization = &common.Notarization{
			Vote: common.ToBeSignedVote{BlockHeader: bh},
			QC:   qc,
		}
	}
	if p.Finalization != nil {
		bh, err := blockHeaderFromProto(p.Finalization.BlockHeader)
		if err != nil {
			return qr, err
		}
		qc, err := qd.DeserializeQuorumCertificate(p.Finalization.Qc.Data)
		if err != nil {
			return qr, fmt.Errorf("quorumRoundFromProto Finalization QC: %w", err)
		}
		qr.Finalization = &common.Finalization{
			Finalization: common.ToBeSignedFinalization{BlockHeader: bh},
			QC:           qc,
		}
	}
	if p.EmptyNotarization != nil {
		en := p.EmptyNotarization
		qc, err := qd.DeserializeQuorumCertificate(en.Qc.Data)
		if err != nil {
			return qr, fmt.Errorf("quorumRoundFromProto EmptyNotarization QC: %w", err)
		}
		qr.EmptyNotarization = &common.EmptyNotarization{
			Vote: common.ToBeSignedEmptyVote{
				EmptyVoteMetadata: common.EmptyVoteMetadata{
					Round: en.Round,
					Epoch: en.Epoch,
				},
			},
			QC: qc,
		}
	}
	return qr, nil
}
