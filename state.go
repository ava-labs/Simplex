package simplex

import "fmt"

type FinalizationCertificatesState struct {
	// sequence number of the last known(most future) finalization certificate
	// we may not be able to store it since this could be greater than maxRoundWindow
	latestRoundKnown Round

	// latest seq we requested
	lastSequenceRequested uint64
}

// SetLastReceivedFCertSeq updates the last received finalization certificate sequence number
// if [seq] is greater than the current last received sequence number
func (fcs *FinalizationCertificatesState) SetLastReceivedFCertSeq(seq uint64) {
	if seq > fcs.latestRoundKnown.num {
		fcs.latestRoundKnown.num = seq
		fcs.latestRoundKnown.block = nil
		fcs.latestRoundKnown.notarization = nil
		fcs.latestRoundKnown.fCert = nil
	}
}

func (fcs *FinalizationCertificatesState) SendFutureCertficatesRequests(start uint64, end uint64, comm Communication) {
	fmt.Println("SendFutureCertficatesRequests", start, end)
	if fcs.lastSequenceRequested >= end {
		// no need to resend
		return
	}

	// we want to collect
	for seq := start; seq <= end; seq++ {
		// also request latest round in case this fCert is also behind
		roundRequest := &Request{
			LatestRoundRequest: &LatestRoundRequest{},
			FinalizationCertificateRequest: &FinalizationCertificateRequest{
				Seq: seq,
			},
		}
		msg := &Message{Request: roundRequest}
		comm.Broadcast(msg)
	}
	fcs.lastSequenceRequested = end
}

func (fcs *FinalizationCertificatesState) ProcessLatestRoundResponse(r *LatestRoundResponse) {
	if r.Block.BlockHeader().Round > fcs.latestRoundKnown.num {
		fcs.latestRoundKnown.num = r.Block.BlockHeader().Round
		fcs.latestRoundKnown.block = r.Block
		fcs.latestRoundKnown.notarization = r.Notarization
		fcs.latestRoundKnown.fCert = r.FCert
	}
}
