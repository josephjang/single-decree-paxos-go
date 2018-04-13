package paxos

import (
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

const channelBufferSize = 1024

//
// Proposer represents a proposer in single-decree Paxos protocol
//
// - name: name of the proposer
// - num: ballot number to propose
// - val: value to request to accept
// - prepareReqChans: acceptors' channels to send PrepareRequest (p1a)
// - acceptReqChans: acceptors' channels to send AcceptRequest (p2a)
// - waitGroup: wait group to notify the proposer process is finished
//
type Proposer struct {
	name            string
	num             int64
	val             int64
	prepareReqChans []chan PrepareRequest
	acceptReqChans  []chan AcceptRequest
	waitGroup       *sync.WaitGroup
}

//
// NewProposer creates a new instance of Proposer
//
func NewProposer(name string, num int64, val int64, prepareReqChans []chan PrepareRequest, acceptReqChans []chan AcceptRequest, wg *sync.WaitGroup) *Proposer {
	p := &Proposer{
		name:            name,
		num:             num,
		val:             val,
		prepareReqChans: prepareReqChans,
		acceptReqChans:  acceptReqChans,
		waitGroup:       wg,
	}

	return p
}

//
// Run executes the proposer's process
//
func (p *Proposer) Run() {
	defer p.waitGroup.Done()

	log.WithFields(log.Fields{"number": p.num, "value": p.val}).Infof("Proposer %s: Started", p.name)

	//
	// Phase 1
	//

	var prepareRespChan = make(chan PrepareResponse, channelBufferSize)

	var prepareRequest = PrepareRequest{
		ProposalNumber:      p.num,
		PrepareResponseChan: prepareRespChan,
	}

	log.Debugf("Proposer %s: Sending prepare request: %+v", p.name, prepareRequest)

	for _, prepareReqChan := range p.prepareReqChans {
		select {
		case prepareReqChan <- prepareRequest:
			log.WithField("number", prepareRequest.ProposalNumber).Infof("Proposer %s: Sent a prepare request", p.name)
		case <-time.After(10 * time.Second):
			log.Infof("Proposer %s: Timeout while sending prepare requests", p.name)
		}
	}

	var prepareResponses []PrepareResponse

	for len(prepareResponses) < 2 {
		select {
		case prepareResp := <-prepareRespChan:
			log.WithFields(
				log.Fields{
					"number":          prepareResp.ProposalNumber,
					"accepted-number": prepareResp.AcceptedProposal.ProposalNumber,
					"accepted-value":  prepareResp.AcceptedProposal.Value,
				}).Infof("Proposer %s: Received a prepare response", p.name)
			prepareResponses = append(prepareResponses, prepareResp)
		case <-time.After(10 * time.Second):
			log.Infof("Proposer %s: Timeout while receiving prepare responses", p.name)
			log.Infof("Proposer %s: Terminated", p.name)
			return
		}
	}

	log.Infof("Proposer %s: Received prepare responses from the majority", p.name)

	//
	// Phase 2
	//

	// check val from the response
	lastProposalFromResp := Proposal{
		ProposalNumber: -1,
	}
	for _, r := range prepareResponses {
		if r.AcceptedProposal.ProposalNumber > lastProposalFromResp.ProposalNumber {
			lastProposalFromResp = r.AcceptedProposal
		}
	}

	if lastProposalFromResp.ProposalNumber != -1 {
		p.val = lastProposalFromResp.Value
	}

	acceptReq := AcceptRequest{
		ProposalNumber: p.num,
		Value:          p.val,
	}

	log.Debugf("Proposer %s: Sending accept request: %+v", p.name, acceptReq)

	for _, acceptReqChan := range p.acceptReqChans {
		select {
		case acceptReqChan <- acceptReq:
			log.WithFields(log.Fields{
				"number": acceptReq.ProposalNumber,
				"value":  acceptReq.Value,
			}).Infof("Proposer %s: Sent a accept request", p.name)
		case <-time.After(10 * time.Second):
			log.Infof("Proposer %s: Timeout while sending accept requests", p.name)
		}
	}

	log.Infof("Proposer %s: Finished", p.name)
}
